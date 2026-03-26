from prefect import task
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterState,
    FlowRunFilterStateType,
    LogFilter,
    LogFilterFlowRunId,
)
import json
import anyio
from google import genai
from datetime import datetime, timedelta
import logging
import os
import requests
import base64
import uuid
import difflib
from typing import Any, Dict, List, Optional


# ============================================================
# CONFIG
# ============================================================
LOOKBACK_MIN = 480
MAX_RUNS = 50
MODEL = "gemini-2.5-flash"
FLOW_BASE_DIR = "/opt/prefect/flows"
GITHUB_REPO = "raullencinaherrera/prefect-gemini-demo"
BASE_BRANCH = "main"
REQUEST_TIMEOUT = 60
VERIFY_SSL = False
MAX_LOG_CHARS = 30000
MAX_CODE_CHARS = 80000
MAX_REPORT_CODE_CHARS = 50000
MAX_PRS_DEFAULT = 5
PREFECT_API_MAX_LIMIT = 200

SYSTEM_FLOW_FILES = {
    "report_failures_flow.py",
    "modify_flow_with_pr_flow.py",
    "auto_fix_failed_flows_flow.py",
    "create_new_flow_with_pr_flow.py",
    "ai_prefect_common.py",
}


# ============================================================
# GENERIC HELPERS
# ============================================================
def truncate_text(text: str, max_chars: int) -> str:
    if not text:
        return ""
    if len(text) <= max_chars:
        return text
    extra = len(text) - max_chars
    return text[:max_chars] + f"\n\n...[TRUNCATED {extra} CHARS]"


def canonical_flow_filename(path: str) -> str:
    if not path:
        return path

    p = path.strip().replace("\\", "/")

    prefixes = [
        "/opt/prefect/flows/",
        "opt/prefect/flows/",
        "./flows/",
        "flows/",
        "/flows/",
        "./",
    ]

    for pref in prefixes:
        if p.startswith(pref):
            p = p[len(pref):]

    return os.path.basename(p)


def normalize_repo_path(filename: str) -> str:
    return f"flows/{canonical_flow_filename(filename)}"


def local_flow_path(filename: str) -> str:
    return os.path.join(FLOW_BASE_DIR, canonical_flow_filename(filename))


def validate_python_code(code: str) -> None:
    compile(code, "<generated>", "exec")


def has_real_diff(original_code: str, final_code: str) -> bool:
    return original_code.replace("\r\n", "\n") != final_code.replace("\r\n", "\n")


def compute_diff(original: str, new: str) -> str:
    diff = difflib.unified_diff(
        original.replace("\r\n", "\n").splitlines(),
        new.replace("\r\n", "\n").splitlines(),
        fromfile="original",
        tofile="updated",
        lineterm="",
    )
    return "\n".join(diff)


def extract_filename_from_entrypoint(entrypoint: str) -> Optional[str]:
    if not entrypoint or entrypoint == "UNKNOWN":
        return None
    raw = entrypoint.split(":")[0]
    return canonical_flow_filename(raw)


def is_system_flow(filename: Optional[str]) -> bool:
    if not filename:
        return False
    return canonical_flow_filename(filename) in SYSTEM_FLOW_FILES


def keep_latest_run_per_flow(runs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}

    for item in runs:
        fname = extract_filename_from_entrypoint(
            item.get("deployment_entrypoint", "UNKNOWN")
        )
        if not fname or is_system_flow(fname):
            continue
        if fname not in latest:
            latest[fname] = item

    return list(latest.values())


def extract_relevant_error(logs: str, max_lines: int = 80) -> str:
    if not logs:
        return ""

    lines = logs.splitlines()

    for i in range(len(lines) - 1, -1, -1):
        line = lines[i]
        if "ERROR" in line or "Exception" in line or "Traceback" in line:
            start = max(0, i - 25)
            end = min(len(lines), i + 55)
            return "\n".join(lines[start:end])

    return "\n".join(lines[-max_lines:])


# ============================================================
# GEMINI
# ============================================================
def gemini_text(prompt: str) -> str:
    client = genai.Client()
    resp = client.models.generate_content(model=MODEL, contents=prompt)
    text = getattr(resp, "text", None)

    if not text or not text.strip():
        raise ValueError("Gemini returned empty response")

    return text.strip()


# ============================================================
# LOAD CODE
# ============================================================
async def load_code_from_prefect(fname: str) -> Optional[str]:
    path = local_flow_path(fname)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    return None


def load_local_code(filename: str) -> str:
    path = local_flow_path(filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Local flow not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def load_code_from_github(filename: str) -> str:
    path = normalize_repo_path(filename)
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    r = github_get(url)
    content = base64.b64decode(r.json()["content"]).decode("utf-8")
    return content


def load_code_best_effort(filename: str) -> str:
    try:
        return load_local_code(filename)
    except Exception:
        return load_code_from_github(filename)


def enrich_runs_with_code(runs_to_analyze: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    enriched = []

    for item in runs_to_analyze:
        row = dict(item)
        entry = row.get("deployment_entrypoint", "UNKNOWN")

        if entry != "UNKNOWN" and not row.get("code"):
            fname = extract_filename_from_entrypoint(entry)
            if fname:
                try:
                    row["code"] = load_code_best_effort(fname)
                except Exception:
                    row["code"] = ""

        enriched.append(row)

    return enriched


# ============================================================
# GITHUB
# ============================================================
def github_headers() -> Dict[str, str]:
    token = os.environ["GITHUB_TOKEN"]
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }


def github_get(url: str) -> requests.Response:
    r = requests.get(
        url,
        headers=github_headers(),
        verify=VERIFY_SSL,
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    return r


def github_post(url: str, payload: Dict[str, Any]) -> requests.Response:
    r = requests.post(
        url,
        headers=github_headers(),
        json=payload,
        verify=VERIFY_SSL,
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    return r


def github_put(url: str, payload: Dict[str, Any]) -> requests.Response:
    r = requests.put(
        url,
        headers=github_headers(),
        json=payload,
        verify=VERIFY_SSL,
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    return r


def get_github_file_info(filename: str) -> Dict[str, Any]:
    path = normalize_repo_path(filename)
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    return github_get(url).json()


def create_pr_for_file(
    filename: str,
    final_code: str,
    analysis: str,
    diff_text: str,
) -> Dict[str, Any]:
    filename = canonical_flow_filename(filename)
    repo_path = normalize_repo_path(filename)
    branch = f"ai-{uuid.uuid4().hex[:8]}"

    base_ref_url = f"https://api.github.com/repos/{GITHUB_REPO}/git/ref/heads/{BASE_BRANCH}"
    base_ref = github_get(base_ref_url).json()
    sha = base_ref["object"]["sha"]

    github_post(
        f"https://api.github.com/repos/{GITHUB_REPO}/git/refs",
        {
            "ref": f"refs/heads/{branch}",
            "sha": sha,
        },
    )

    current_info = get_github_file_info(filename)

    github_put(
        f"https://api.github.com/repos/{GITHUB_REPO}/contents/{repo_path}",
        {
            "message": f"AI modification to {filename}",
            "content": base64.b64encode(final_code.encode("utf-8")).decode("utf-8"),
            "sha": current_info["sha"],
            "branch": branch,
        },
    )

    pr_body = (
        "Automated change from Prefect + Gemini\n\n"
        "## Summary\n"
        f"{analysis}\n\n"
        "## Diff preview\n"
        f"```diff\n{diff_text}\n```"
    )

    pr = github_post(
        f"https://api.github.com/repos/{GITHUB_REPO}/pulls",
        {
            "title": f"AI modification to {filename}",
            "body": pr_body,
            "head": branch,
            "base": BASE_BRANCH,
        },
    )

    return {
        "filename": filename,
        "status": "pr_created",
        "pr_created": True,
        "pr_url": pr.json()["html_url"],
    }


# ============================================================
# FULL FILE GENERATION
# ============================================================
def generate_full_code(user_prompt: str, code: str, logs: str = "") -> str:
    prompt = f"""
You must rewrite the FULL Python file applying the requested change.

User request:
\"\"\"{user_prompt}\"\"\"

Relevant logs:
\"\"\"text
{truncate_text(logs or "", MAX_LOG_CHARS)}
\"\"\"

Original file:
\"\"\"python
{truncate_text(code, MAX_CODE_CHARS)}
\"\"\"

Rules:
- Return ONLY the FULL updated Python file
- Do NOT explain anything
- Do NOT remove unrelated code
- Keep structure intact unless the user asks otherwise
- Keep imports unless they become clearly unnecessary because of your own change
- Keep Prefect decorators
- Ensure the code is executable
- Apply the requested optimization/fix in the code itself, not only in theory
- Prefer minimal real code changes, but return the whole file
"""
    result = gemini_text(prompt)

    if not result.strip():
        raise ValueError("Gemini returned empty code")

    cleaned = result.strip()

    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        cleaned = "\n".join(lines).strip()

    return cleaned


def generate_fix_code_from_failure(user_prompt: str, code: str, logs: str = "") -> str:
    prompt = f"""
You must rewrite the FULL Python file to fix the failure described in the logs.

User request:
\"\"\"{user_prompt}\"\"\"

Relevant logs:
\"\"\"text
{truncate_text(logs or "", MAX_LOG_CHARS)}
\"\"\"

Original file:
\"\"\"python
{truncate_text(code, MAX_CODE_CHARS)}
\"\"\"

Rules:
- Return ONLY the FULL updated Python file
- Do NOT explain anything
- Preserve unrelated logic
- Make the minimum real code changes needed to fix the failure
- Keep the code executable
- Keep Prefect decorators and overall structure
"""
    result = gemini_text(prompt)

    if not result.strip():
        raise ValueError("Gemini returned empty code")

    cleaned = result.strip()

    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        cleaned = "\n".join(lines).strip()

    return cleaned


# ============================================================
# DETERMINISTIC DEMO FIXES
# ============================================================
def try_rule_based_full_rewrite(
    filename: str,
    original_code: str,
    user_prompt: str,
    logs: str = "",
) -> Optional[str]:
    fname = canonical_flow_filename(filename)
    prompt_l = (user_prompt or "").lower()
    logs_l = (logs or "").lower()

    # --------------------------------------------------------
    # device_loader.py
    # --------------------------------------------------------
    if fname == "device_loader.py":
        if "router" in prompt_l and (
            "remove" in prompt_l
            or "exclude" in prompt_l
            or "without" in prompt_l
            or "quita" in prompt_l
            or "elimina" in prompt_l
        ):
            old = """DEVICES = [
    "router-01", "router-02", "router-23",
    "switch-05", "switch-11", "switch-42",
    "fw-01", "fw-02",
]"""
            new = """DEVICES = [
    "switch-05", "switch-11", "switch-42",
    "fw-01", "fw-02",
]"""
            if old in original_code:
                return original_code.replace(old, new, 1)

    # --------------------------------------------------------
    # fail_division_by_zero.py
    # --------------------------------------------------------
    if fname == "fail_division_by_zero.py":
        if "division by zero" in prompt_l or "zerodivisionerror" in prompt_l or "divide by zero" in prompt_l or "optimize" in prompt_l or "optimiza" in prompt_l or "optimice" in prompt_l or "zero" in logs_l:
            code = original_code

            code = code.replace(
                '# Tarea auxiliar completamente inútil\n@task\ndef useless_wait():\n    # Espera "por si acaso"\n    time.sleep(random.choice([0.1, 0.2, 0.3]))\n    return "espera completada aunque no sirve para nada"\n\n',
                "",
            )
            code = code.replace(
                '# Otra tarea absurda que solo envuelve un valor sin modificarlo\n@task\ndef wrap_value(value):\n    wrapped = {"value": value}\n    return wrapped\n\n',
                "",
            )
            code = code.replace(
                '    # Simulación inútil de cálculo "complejo"\n    basura = []\n    for _ in range(100000):  # 100k operaciones totalmente inútiles\n        basura.append(random.random() ** 2)\n\n',
                "",
            )
            code = code.replace(
                '    # Log redundante\n    print(f"Voy a dividir 10 entre {x}, espero que no explote...")\n\n',
                '    logger = get_run_logger()\n    logger.info(f"Voy a dividir 10 entre {x}...")\n\n',
            )
            code = code.replace(
                "    return 10 / x\n",
                "    return 10 / x if x != 0 else None\n",
            )
            code = code.replace(
                '    # Llamadas inútiles antes de hacer lo que importa\n    useless = useless_wait()\n    logger.info(f"Resultado de tarea inútil: {useless}")\n\n    wrapped_random = wrap_value(random.randint(1, 5))\n    logger.info(f"Valor envuelto irrelevante: {wrapped_random}")\n\n',
                "",
            )
            code = code.replace(
                '    logger.info(f"El flujo ha terminado aunque no hiciera falta tanto esfuerzo... Resultado: {val}")\n',
                '    logger.info(f"Flujo completado. Resultado: {val}")\n',
            )

            if code != original_code:
                return code

    return None


# ============================================================
# PREFECT RUNS
# ============================================================
@task
def get_recent_runs(
    lookback_min: int = LOOKBACK_MIN,
    max_runs: int = MAX_RUNS,
    state_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    async def _fetch():
        since = datetime.utcnow() - timedelta(minutes=lookback_min)
        out: List[Dict[str, Any]] = []

        effective_limit = min(max_runs, PREFECT_API_MAX_LIMIT)

        async with get_client() as client:
            flow_run_filter = None

            if state_type:
                flow_run_filter = FlowRunFilter(
                    state=FlowRunFilterState(
                        type=FlowRunFilterStateType(any_=[state_type])
                    )
                )

            runs = await client.read_flow_runs(
                flow_run_filter=flow_run_filter,
                sort="START_TIME_DESC",
                limit=effective_limit,
            )

            for r in runs:
                if not r.start_time or r.start_time.replace(tzinfo=None) < since:
                    continue

                lf = LogFilter(flow_run_id=LogFilterFlowRunId(any_=[str(r.id)]))
                logs = await client.read_logs(log_filter=lf)

                log_text = "\n".join(
                    f"[{l.timestamp}] {logging.getLevelName(int(l.level))}: {l.message}"
                    for l in logs
                )

                entry = "UNKNOWN"
                code = None

                if r.deployment_id:
                    try:
                        dep = await client.read_deployment(r.deployment_id)
                        entry = dep.entrypoint
                    except Exception:
                        entry = "UNKNOWN"
                    fname = extract_filename_from_entrypoint(entry)
                    if fname:
                        code = await load_code_from_prefect(fname)
                    else:
                        continue

                out.append(
                    {
                        "flow_run_id": str(r.id),
                        "flow_run_name": r.name,
                        "state_name": getattr(r.state, "name", None)
                        if r.state
                        else None,
                        "deployment_entrypoint": entry,
                        "logs": log_text,
                        "code": code,
                    }
                )

        return out

    return anyio.run(_fetch)



@task
def get_failed_runs(
    lookback_min: int = LOOKBACK_MIN,
    max_runs: int = MAX_RUNS,
) -> List[Dict[str, Any]]:
    return get_recent_runs.fn(
        lookback_min=lookback_min,
        max_runs=max_runs,
        state_type="FAILED",
    )


# ============================================================
# REPORTS
# ============================================================
@task
def build_report(user_prompt: str, runs_to_analyze: List[Dict[str, Any]]) -> str:
    sections = []

    for item in runs_to_analyze:
        entrypoint = item.get("deployment_entrypoint", "UNKNOWN")
        fname = extract_filename_from_entrypoint(entrypoint) or "UNKNOWN"
        code = truncate_text(item.get("code") or "", MAX_REPORT_CODE_CHARS)
        logs = truncate_text(item.get("logs") or "", MAX_LOG_CHARS)
        state_name = item.get("state_name") or "UNKNOWN"

        sections.append(
            f"""
FLOW FILE: {fname}
FLOW RUN ID: {item.get("flow_run_id")}
FLOW RUN NAME: {item.get("flow_run_name")}
STATE: {state_name}

CODE:
\"\"\"python
{code}
\"\"\"

LOGS:
\"\"\"text
{logs}
\"\"\"
"""
        )

    joined = "\n\n============================\n\n".join(sections)

    prompt = f"""
User request:
\"\"\"{user_prompt}\"\"\"

You are analyzing Prefect flow runs.

IMPORTANT:
- Focus only on what the user explicitly asked.
- If the user asks about recent runs, analyze only the provided recent runs.
- If the user asks about failures, focus only on failures.
- Do not expand into a broad general audit unless the user asked for it.
- Be concise, technical, and centered on the requested problem.

For each relevant flow/run:
- probable root cause
- evidence from logs
- whether the issue looks code-related, infra-related, config-related, external dependency-related, or transient
- concrete next step

At the end include:
- short summary
- priority recommendation

DATA:
{joined}
"""
    return gemini_text(prompt)


# ============================================================
# MODIFY FLOW
# ============================================================
@task
def analyze_single_flow_for_modification(user_prompt: str, code: str, logs: str) -> str:
    prompt = f"""
User request:
\"\"\"{user_prompt}\"\"\"

Analyze strictly based on the code and the logs provided.
Respond in the same language as the user's request.
Be concrete and technical.

CODE:
\"\"\"python
{truncate_text(code, MAX_CODE_CHARS)}
\"\"\"

LOGS:
\"\"\"text
{truncate_text(logs, MAX_LOG_CHARS)}
\"\"\"
"""
    return gemini_text(prompt)


@task
def prepare_modification_for_single_flow(
    user_prompt: str,
    flow_file: str,
    code: str = "",
    logs: str = "",
) -> Dict[str, Any]:
    filename = canonical_flow_filename(flow_file)

    try:
        original_code = load_local_code(filename)
    except Exception as e:
        return {
            "filename": filename,
            "status": "skipped",
            "reason": f"Local load failed: {e}",
        }

    try:
        analysis = analyze_single_flow_for_modification.fn(
            user_prompt,
            original_code,
            logs,
        )
    except Exception as e:
        return {
            "filename": filename,
            "status": "skipped",
            "reason": f"Analysis failed: {e}",
        }

    try:
        new_code = try_rule_based_full_rewrite(
            filename=filename,
            original_code=original_code,
            user_prompt=user_prompt,
            logs=logs,
        )
        if new_code is None:
            new_code = generate_full_code(user_prompt, original_code, logs)
    except Exception as e:
        return {
            "filename": filename,
            "status": "skipped",
            "reason": f"LLM generation failed: {e}",
            "analysis": analysis,
        }

    try:
        validate_python_code(new_code)
    except Exception as e:
        return {
            "filename": filename,
            "status": "skipped",
            "reason": f"Generated code invalid: {e}",
            "analysis": analysis,
        }

    diff_text = compute_diff(original_code, new_code)

    if not diff_text.strip():
        return {
            "filename": filename,
            "status": "no_changes",
            "reason": "No diff detected",
            "analysis": analysis,
            "original_code": original_code,
            "final_code": new_code,
            "diff": "",
        }

    return {
        "filename": filename,
        "status": "ready",
        "analysis": analysis,
        "original_code": original_code,
        "final_code": new_code,
        "diff": diff_text,
    }


# ============================================================
# AUTO-FIX FAILED RUNS
# ============================================================
@task
def prepare_modifications_for_failed_runs(
    user_prompt: str,
    runs_to_modify: List[Dict[str, Any]],
    max_prs: int = MAX_PRS_DEFAULT,
) -> List[Dict[str, Any]]:
    outputs: List[Dict[str, Any]] = []
    ready_count = 0

    for item in runs_to_modify:
        entry = item.get("deployment_entrypoint", "UNKNOWN")
        fname = extract_filename_from_entrypoint(entry)

        if not fname:
            outputs.append(
                {
                    "filename": None,
                    "status": "skipped",
                    "reason": "UNKNOWN entrypoint",
                }
            )
            continue

        if is_system_flow(fname):
            outputs.append(
                {
                    "filename": fname,
                    "status": "skipped",
                    "reason": "System flow excluded from auto-fix",
                }
            )
            continue

        if ready_count >= max_prs:
            outputs.append(
                {
                    "filename": fname,
                    "status": "skipped",
                    "reason": f"max_prs limit reached ({max_prs})",
                }
            )
            continue

        try:
            original_code = load_local_code(fname)
        except Exception as e:
            outputs.append(
                {
                    "filename": fname,
                    "status": "skipped",
                    "reason": f"Local load failed: {e}",
                }
            )
            continue

        logs = extract_relevant_error(item.get("logs") or "")

        try:
            analysis = analyze_single_flow_for_modification.fn(
                user_prompt,
                original_code,
                logs,
            )
        except Exception as e:
            outputs.append(
                {
                    "filename": fname,
                    "status": "skipped",
                    "reason": f"Analysis failed: {e}",
                }
            )
            continue

        try:
            new_code = try_rule_based_full_rewrite(
                filename=fname,
                original_code=original_code,
                user_prompt=user_prompt,
                logs=logs,
            )
            if new_code is None:
                new_code = generate_fix_code_from_failure(
                    user_prompt=user_prompt,
                    code=original_code,
                    logs=logs,
                )
        except Exception as e:
            outputs.append(
                {
                    "filename": fname,
                    "status": "skipped",
                    "reason": f"Code generation failed: {e}",
                    "analysis": analysis,
                }
            )
            continue

        try:
            validate_python_code(new_code)
        except Exception as e:
            outputs.append(
                {
                    "filename": fname,
                    "status": "skipped",
                    "reason": f"Generated code invalid: {e}",
                    "analysis": analysis,
                }
            )
            continue

        diff_text = compute_diff(original_code, new_code)

        if not diff_text.strip():
            outputs.append(
                {
                    "filename": fname,
                    "status": "no_changes",
                    "reason": "No diff detected",
                    "analysis": analysis,
                    "original_code": original_code,
                    "final_code": new_code,
                    "diff": "",
                }
            )
            continue

        outputs.append(
            {
                "filename": fname,
                "status": "ready",
                "analysis": analysis,
                "original_code": original_code,
                "final_code": new_code,
                "diff": diff_text,
            }
        )
        ready_count += 1

    return outputs


# ============================================================
# PR CREATION
# ============================================================
@task
def create_prs_for_modifications(modifications: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    created = []

    for mod in modifications:
        if mod.get("status") != "ready":
            created.append(
                {
                    "filename": mod.get("filename"),
                    "status": mod.get("status"),
                    "pr_created": False,
                }
            )
            continue

        created.append(
            create_pr_for_file(
                filename=mod["filename"],
                final_code=mod["final_code"],
                analysis=mod.get("analysis", ""),
                diff_text=mod.get("diff", ""),
            )
        )

    return created

def safe_json_load(text: str) -> dict:
    raw = text.strip()

    if raw.startswith("```"):
        lines = raw.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        raw = "\n".join(lines).strip()

    start = raw.find("{")
    end = raw.rfind("}")

    if start == -1 or end == -1 or end <= start:
        raise ValueError(f"No valid JSON object found in response:\n{raw}")

    return json.loads(raw[start:end + 1])


def generate_new_flow_artifact(user_prompt: str) -> dict:
    prompt = f"""
You are an expert Prefect engineer.

Create a complete new Prefect flow based on this request:

{user_prompt}

Requirements:
- Return a FULL working Python file
- Must include @flow
- Use @task if appropriate
- Clean and simple code
- Production-style structure
- No explanations outside JSON

Return ONLY valid JSON with this exact schema:
{{
  "filename": "new_flow_name.py",
  "code": "FULL PYTHON FILE CONTENT"
}}
"""

    result = gemini_text(prompt)
    data = safe_json_load(result)

    filename = data.get("filename", "").strip()
    code = data.get("code", "")

    if not filename:
        raise ValueError("Gemini did not return filename")
    if not code.strip():
        raise ValueError("Gemini did not return code")

    filename = canonical_flow_filename(filename)
    validate_python_code(code)

    diff_text = f"--- /dev/null\n+++ {filename}\n"

    return {
        "filename": filename,
        "status": "ready",
        "analysis": f"New flow generated from prompt: {filename}",
        "final_code": code,
        "diff": diff_text,
    }


def create_pr_for_new_file(
    filename: str,
    final_code: str,
    analysis: str,
    diff_text: str,
) -> Dict[str, Any]:
    filename = canonical_flow_filename(filename)
    repo_path = normalize_repo_path(filename)
    branch = f"ai-{uuid.uuid4().hex[:8]}"

    base_ref_url = f"https://api.github.com/repos/{GITHUB_REPO}/git/ref/heads/{BASE_BRANCH}"
    base_ref = github_get(base_ref_url).json()
    sha = base_ref["object"]["sha"]

    github_post(
        f"https://api.github.com/repos/{GITHUB_REPO}/git/refs",
        {
            "ref": f"refs/heads/{branch}",
            "sha": sha,
        },
    )

    payload = {
        "message": f"AI create {filename}",
        "content": base64.b64encode(final_code.encode("utf-8")).decode("utf-8"),
        "branch": branch,
    }

    # Si el fichero ya existe en GitHub, lo actualiza; si no, lo crea.
    try:
        current_info = get_github_file_info(filename)
        payload["sha"] = current_info["sha"]
    except Exception:
        pass

    github_put(
        f"https://api.github.com/repos/{GITHUB_REPO}/contents/{repo_path}",
        payload,
    )

    pr_body = (
        "Automated new flow creation from Prefect + Gemini\n\n"
        "## Summary\n"
        f"{analysis}\n\n"
        "## Diff preview\n"
        f"```diff\n{diff_text}\n```"
    )

    pr = github_post(
        f"https://api.github.com/repos/{GITHUB_REPO}/pulls",
        {
            "title": f"AI create new flow: {filename}",
            "body": pr_body,
            "head": branch,
            "base": BASE_BRANCH,
        },
    )

    return {
        "filename": filename,
        "status": "pr_created",
        "pr_created": True,
        "pr_url": pr.json()["html_url"],
    }
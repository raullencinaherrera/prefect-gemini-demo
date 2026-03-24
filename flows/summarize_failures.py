from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterState,
    FlowRunFilterStateType,
    LogFilter,
    LogFilterFlowRunId,
)

import anyio
from datetime import datetime, timedelta
from google import genai
import logging
import os
import requests
import base64
import uuid
import json
import ast
import re
from typing import Any, Dict, List, Optional


# ============================================================
# CONFIG
# ============================================================
LOOKBACK_MIN = 480
MAX_RUNS = 20
MODEL = "gemini-2.5-flash"
FLOW_BASE_DIR = "/opt/prefect/flows"
GITHUB_REPO = "raullencinaherrera/prefect-gemini-demo"
BASE_BRANCH = "main"
REQUEST_TIMEOUT = 60
VERIFY_SSL = False
MAX_LOG_CHARS = 30000
MAX_CODE_CHARS = 50000


# ============================================================
# HELPERS
# ============================================================
def normalize_repo_path(filename: str) -> str:
    filename = filename.strip().replace("\\", "/")
    for pref in [
        "/opt/prefect/flows/",
        "opt/prefect/flows/",
        "flows/",
        "/flows/",
        "./flows/",
        "./",
        "/",
    ]:
        if filename.startswith(pref):
            filename = filename[len(pref):]
    return f"flows/{filename}"


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


def truncate_text(text: str, max_chars: int) -> str:
    if not text:
        return ""
    if len(text) <= max_chars:
        return text
    extra = len(text) - max_chars
    return text[:max_chars] + f"\n\n...[TRUNCATED {extra} CHARS]"


def safe_json_dumps(obj: Any) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=False)


def extract_fenced_block(text: str) -> str:
    raw = text.strip()
    if raw.startswith("```"):
        lines = raw.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        return "\n".join(lines).strip()
    return raw


def extract_json_object(text: str) -> Dict[str, Any]:
    raw = extract_fenced_block(text)
    decoder = json.JSONDecoder()

    for i, ch in enumerate(raw):
        if ch != "{":
            continue
        try:
            obj, _ = decoder.raw_decode(raw[i:])
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            continue

    raise ValueError(f"No valid JSON object found in model response:\n{raw}")


def repair_json_with_ast(raw_text: str) -> Optional[Dict[str, Any]]:
    raw = extract_fenced_block(raw_text)

    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None

    candidate = raw[start : end + 1]

    relaxed = candidate
    relaxed = relaxed.replace("\r\n", "\n")
    relaxed = re.sub(r"\btrue\b", "True", relaxed)
    relaxed = re.sub(r"\bfalse\b", "False", relaxed)
    relaxed = re.sub(r"\bnull\b", "None", relaxed)

    try:
        obj = ast.literal_eval(relaxed)
        if isinstance(obj, dict):
            return obj
    except Exception:
        return None

    return None


def gemini_text(prompt: str) -> str:
    client = genai.Client()
    resp = client.models.generate_content(model=MODEL, contents=prompt)
    text = getattr(resp, "text", None)

    if not text or not text.strip():
        raise ValueError("Gemini returned empty response")

    return text.strip()


def gemini_json(prompt: str, max_attempts: int = 3) -> Dict[str, Any]:
    last_error = None
    previous_response = None

    for attempt in range(1, max_attempts + 1):
        if attempt == 1:
            text = gemini_text(prompt)
        else:
            repair_prompt = f"""
Your previous response was not valid JSON.

Return ONLY valid JSON and nothing else.
Do not use markdown.
Do not add explanations.

Original instruction:
{prompt}

Previous invalid response:
{previous_response}
"""
            text = gemini_text(repair_prompt)

        previous_response = text

        try:
            return extract_json_object(text)
        except Exception as e1:
            last_error = e1

            repaired = repair_json_with_ast(text)
            if repaired is not None:
                return repaired

    raise ValueError(f"Could not parse Gemini JSON after {max_attempts} attempts: {last_error}")


def b64_decode_text(value: str) -> str:
    try:
        return base64.b64decode(value).decode("utf-8")
    except Exception as e:
        raise ValueError(f"Invalid base64 text: {e}")


def validate_change_schema(changes: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(changes, dict):
        raise ValueError("Changes payload must be a JSON object")

    remove = changes.get("remove", [])
    replace = changes.get("replace", [])
    add = changes.get("add", [])

    if not isinstance(remove, list):
        raise ValueError("'remove' must be a list")
    if not isinstance(replace, list):
        raise ValueError("'replace' must be a list")
    if not isinstance(add, list):
        raise ValueError("'add' must be a list")

    for i, item in enumerate(remove):
        if not isinstance(item, str):
            raise ValueError(f"'remove[{i}]' must be a base64 string")

    for i, item in enumerate(replace):
        if not isinstance(item, dict):
            raise ValueError(f"'replace[{i}]' must be an object")
        if "old_b64" not in item or "new_b64" not in item:
            raise ValueError(f"'replace[{i}]' must contain 'old_b64' and 'new_b64'")
        if not isinstance(item["old_b64"], str) or not isinstance(item["new_b64"], str):
            raise ValueError(f"'replace[{i}].old_b64' and 'replace[{i}].new_b64' must be strings")

    for i, item in enumerate(add):
        if not isinstance(item, dict):
            raise ValueError(f"'add[{i}]' must be an object")
        if "after_b64" not in item or "content_b64" not in item:
            raise ValueError(f"'add[{i}]' must contain 'after_b64' and 'content_b64'")
        if not isinstance(item["after_b64"], str) or not isinstance(item["content_b64"], str):
            raise ValueError(f"'add[{i}].after_b64' and 'add[{i}].content_b64' must be strings")

    return {
        "remove": remove,
        "replace": replace,
        "add": add,
    }


def apply_json_changes(original: str, changes: Dict[str, Any]) -> str:
    result = original

    for block_b64 in changes.get("remove", []):
        block = b64_decode_text(block_b64)
        if not block:
            continue
        if block not in result:
            raise ValueError(f"Remove block not found in file:\n{block}")
        result = result.replace(block, "", 1)

    for rep in changes.get("replace", []):
        old = b64_decode_text(rep.get("old_b64", ""))
        new = b64_decode_text(rep.get("new_b64", ""))

        if not old:
            raise ValueError("Replace operation has empty 'old_b64'")
        if old not in result:
            raise ValueError(f"Replace target not found in file:\n{old}")

        result = result.replace(old, new, 1)

    for item in changes.get("add", []):
        after = b64_decode_text(item.get("after_b64", ""))
        content = b64_decode_text(item.get("content_b64", ""))

        if not after:
            raise ValueError("Add operation has empty 'after_b64'")
        if not content:
            raise ValueError("Add operation has empty 'content_b64'")
        if after not in result:
            raise ValueError(f"Add anchor not found in file:\n{after}")

        result = result.replace(after, after + content, 1)

    return result


def validate_python_code(code: str) -> None:
    compile(code, "<generated>", "exec")


async def load_code_from_prefect(fname: str) -> Optional[str]:
    path = os.path.join(FLOW_BASE_DIR, fname)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    return None


def load_code_from_github(filename: str) -> str:
    rp = normalize_repo_path(filename)
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{rp}"
    r = github_get(url)
    content = base64.b64decode(r.json()["content"]).decode("utf-8")
    return content


def get_github_file_info(filename: str) -> Dict[str, Any]:
    rp = normalize_repo_path(filename)
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{rp}"
    r = github_get(url)
    return r.json()


def load_code_best_effort(filename: str) -> str:
    local_path = os.path.join(FLOW_BASE_DIR, filename)
    if os.path.exists(local_path):
        with open(local_path, "r", encoding="utf-8") as f:
            return f.read()
    return load_code_from_github(filename)


def enrich_runs_with_code(runs_to_analyze: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    enriched = []

    for r in runs_to_analyze:
        item = dict(r)
        entry = item.get("deployment_entrypoint", "UNKNOWN")

        if entry != "UNKNOWN":
            fname = entry.split(":")[0]
            if not item.get("code"):
                try:
                    item["code"] = load_code_best_effort(fname)
                except Exception:
                    item["code"] = ""

        enriched.append(item)

    return enriched


def has_explicit_pr_request(user_prompt: str) -> bool:
    p = user_prompt.lower()

    explicit_positive = [
        "create a pull request",
        "open a pull request",
        "generate a pull request",
        "make a pull request",
        "create pull request",
        "open pull request",
        "generate pull request",
        "make pull request",
        "haz un pull request",
        "crea un pull request",
        "abre un pull request",
        "genera un pull request",
        "hazme un pull request",
        "crea un pr",
        "abre un pr",
        "genera un pr",
        "make a pr",
        "create a pr",
        "open a pr",
    ]

    explicit_negative = [
        "do not create a pull request",
        "don't create a pull request",
        "no pull request",
        "without pull request",
        "sin pull request",
        "no generes pull request",
        "no crees pull request",
        "no abras pull request",
        "sin pr",
        "no pr",
    ]

    if any(x in p for x in explicit_negative):
        return False

    return any(x in p for x in explicit_positive)


def detect_explicit_modify_request(user_prompt: str) -> bool:
    p = user_prompt.lower()

    positive_modify = [
        "fix ",
        "fix the",
        "modify ",
        "modify the",
        "update ",
        "update the",
        "change ",
        "change the",
        "edit ",
        "edit the",
        "patch ",
        "remove ",
        "add ",
        "correct ",
        "refactor ",
        "arregla",
        "corrige",
        "modifica",
        "actualiza",
        "cambia",
        "edita",
        "elimina",
        "añade",
        "agrega",
        "refactoriza",
        "haz un cambio",
        "haz cambios",
    ]

    negative_modify = [
        "do not modify",
        "don't modify",
        "without modifying",
        "sin modificar",
        "no modifiques",
        "no cambies el código",
        "solo analiza",
        "solo analiza los fallos",
        "solo informe",
        "solo report",
        "solo recomendaciones",
        "only analyze",
        "analysis only",
        "report only",
        "recommendations only",
    ]

    if any(x in p for x in negative_modify):
        return False

    return any(x in p for x in positive_modify)


def detect_explicit_report_request(user_prompt: str) -> bool:
    p = user_prompt.lower()

    report_terms = [
        "analyze",
        "analyse",
        "report",
        "summary",
        "summarize",
        "diagnosis",
        "diagnose",
        "recommendations",
        "root cause",
        "informe",
        "analiza",
        "analizar",
        "resumen",
        "diagnóstico",
        "causa raíz",
        "recomendaciones",
        "explica",
        "explicación",
        "fallos",
        "errores",
    ]

    modify_terms = [
        "fix ",
        "modify ",
        "update ",
        "change ",
        "edit ",
        "patch ",
        "remove ",
        "add ",
        "correct ",
        "refactor ",
        "arregla",
        "corrige",
        "modifica",
        "actualiza",
        "cambia",
        "edita",
        "elimina",
        "añade",
        "agrega",
        "refactoriza",
    ]

    has_report = any(x in p for x in report_terms)
    has_modify = any(x in p for x in modify_terms)

    return has_report and not has_modify


def detect_all_flows_request(user_prompt: str) -> bool:
    p = user_prompt.lower()
    terms = [
        "all flows",
        "all failed flows",
        "all prefect flows",
        "todos los flujos",
        "todos los flows",
        "todos los fallos",
        "todos los errores",
        "todos los flujos de prefect",
        "todos los flujos fallidos",
        "todos los run failures",
        "todos los failed runs",
    ]
    return any(x in p for x in terms)


def resolve_action(user_prompt: str, llm_action: str) -> str:
    explicit_modify = detect_explicit_modify_request(user_prompt)
    explicit_report = detect_explicit_report_request(user_prompt)
    explicit_pr = has_explicit_pr_request(user_prompt)

    if explicit_pr:
        return "modify"

    if explicit_modify:
        return "modify"

    if explicit_report:
        return "report"

    if llm_action in {"report", "modify"}:
        return llm_action

    return "report"


def summarize_modifications(modifications: List[Dict[str, Any]]) -> str:
    lines = []
    for mod in modifications:
        fname = mod.get("filename")
        status = mod.get("status")

        if status == "ready":
            lines.append(f"- {fname}: ready")
        elif status == "no_changes":
            lines.append(f"- {fname}: no_changes")
        else:
            reason = mod.get("reason", "unknown")
            lines.append(f"- {fname}: {status} ({reason})")

    if not lines:
        return "No modifications generated."

    return "Proposed modifications:\n" + "\n".join(lines)


# ============================================================
# TASK 1 — GET FAILED RUNS
# ============================================================
@task
def get_failed_runs() -> List[Dict[str, Any]]:
    async def _fetch():
        since = datetime.utcnow() - timedelta(minutes=LOOKBACK_MIN)
        out: List[Dict[str, Any]] = []

        async with get_client() as client:
            runs = await client.read_flow_runs(
                flow_run_filter=FlowRunFilter(
                    state=FlowRunFilterState(
                        type=FlowRunFilterStateType(any_=["FAILED"])
                    )
                ),
                sort="START_TIME_DESC",
                limit=MAX_RUNS,
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
                    dep = await client.read_deployment(r.deployment_id)
                    entry = dep.entrypoint
                    fname = entry.split(":")[0]
                    code = await load_code_from_prefect(fname)

                out.append(
                    {
                        "flow_run_id": str(r.id),
                        "flow_run_name": r.name,
                        "deployment_entrypoint": entry,
                        "logs": truncate_text(log_text, MAX_LOG_CHARS),
                        "code": truncate_text(code or "", MAX_CODE_CHARS) if code else code,
                    }
                )

        return out

    return anyio.run(_fetch)


# ============================================================
# TASK 2 — DETECT SCOPE WITH LLM
# ============================================================
@task
def detect_intent(user_prompt: str) -> Dict[str, Any]:
    prompt = f"""
You are classifying a user's request about failed Prefect flows.

User said:
\"\"\"{user_prompt}\"\"\"

Return ONLY valid JSON with this exact schema:
{{
  "mode": "single",
  "flows": [],
  "action": "report"
}}

Rules:
- mode must be either "single" or "all"
- flows must be an array of filenames like ["flow_a.py", "flow_b.py"]
- use an empty array if no specific flow names were requested
- action must be either "report" or "modify"
- choose action="report" for requests asking for analysis, diagnosis, informe, summary, explanation, report, root cause, recomendaciones
- choose action="modify" only if the user explicitly asks to change code, fix code, remove code, add code, edit code, update code, patch code
- if the user asks about all flows, use mode="all"
- do not decide about pull requests
- no markdown
- no explanations
"""
    try:
        data = gemini_json(prompt)

        mode = data.get("mode", "single")
        flows = data.get("flows", [])
        action = data.get("action", "report")

        if mode not in {"single", "all"}:
            mode = "single"

        if not isinstance(flows, list):
            flows = []

        flows = [x for x in flows if isinstance(x, str) and x.strip()]
        action = action if action in {"report", "modify"} else "report"

        return {
            "mode": mode,
            "flows": flows,
            "action": action,
        }

    except Exception:
        return {
            "mode": "single",
            "flows": [],
            "action": "report",
        }


# ============================================================
# TASK 3 — SELECT TARGET RUNS
# ============================================================
@task
def select_target_runs(
    failed_runs: List[Dict[str, Any]],
    mode: str,
    requested_flows: List[str],
) -> List[Dict[str, Any]]:
    if not failed_runs:
        return []

    if mode == "all":
        return failed_runs

    if requested_flows:
        normalized = {f.strip() for f in requested_flows}
        selected = []

        for r in failed_runs:
            entry = r.get("deployment_entrypoint", "UNKNOWN")
            if entry == "UNKNOWN":
                continue
            fname = entry.split(":")[0]
            if fname in normalized:
                selected.append(r)

        if selected:
            return selected

    return [failed_runs[0]]


# ============================================================
# TASK 4 — BUILD REPORT
# ============================================================
@task
def build_report(user_prompt: str, runs_to_analyze: List[Dict[str, Any]]) -> str:
    sections = []

    for item in runs_to_analyze:
        entrypoint = item.get("deployment_entrypoint", "UNKNOWN")
        fname = entrypoint.split(":")[0] if entrypoint != "UNKNOWN" else "UNKNOWN"
        code = item.get("code") or ""
        logs = item.get("logs") or ""

        sections.append(
            f"""
FLOW FILE: {fname}
FLOW RUN ID: {item.get("flow_run_id")}
FLOW RUN NAME: {item.get("flow_run_name")}

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

You are analyzing multiple failed Prefect flows.

Write a structured technical report in the same language as the user's request.

For each flow:
- probable root cause
- evidence from logs
- whether the issue looks code-related, infra-related, config-related, external dependency-related, or transient
- concrete next steps

At the end include:
- overall summary
- repeated patterns across flows
- recommended priority order

DATA:
{joined}
"""
    return gemini_text(prompt)


# ============================================================
# TASK 5 — ANALYZE SINGLE FLOW FOR MODIFICATION
# ============================================================
@task
def analyze_single_flow_for_modification(user_prompt: str, code: str, logs: str) -> str:
    prompt = f"""
User request:
\"\"\"{user_prompt}\"\"\"

Analyze strictly based on CODE and LOGS.
Be concrete and technical.
Respond in the same language as the user's request.

CODE:
\"\"\"python
{code}
\"\"\"

LOGS:
\"\"\"text
{logs}
\"\"\"
"""
    return gemini_text(prompt)


# ============================================================
# TASK 6 — GET CHANGES
# ============================================================
@task(cache_policy=NO_CACHE)
def get_semantic_changes(user_prompt: str, code: str) -> Dict[str, Any]:
    prompt = f"""
You must generate safe source-code edits for a Python file.

User request:
\"\"\"{user_prompt}\"\"\"

Full file:
\"\"\"python
{code}
\"\"\"

Return ONLY valid JSON with this exact schema:
{{
  "remove": [
    "base64_encoded_exact_block_to_remove"
  ],
  "replace": [
    {{
      "old_b64": "base64_encoded_exact_old_block",
      "new_b64": "base64_encoded_exact_new_block"
    }}
  ],
  "add": [
    {{
      "after_b64": "base64_encoded_exact_anchor_text_already_present_in_file",
      "content_b64": "base64_encoded_content_to_insert_immediately_after_anchor"
    }}
  ]
}}

Rules:
- output JSON only
- no markdown fences
- no explanations
- do not return the whole file
- encode every code fragment as base64 UTF-8
- all old/after values must decode to exact text already present in the file
- do not invent anchors
- prefer replace over remove+add when modifying existing code
- if no changes are needed, return empty lists
"""
    parsed = gemini_json(prompt, max_attempts=3)
    return validate_change_schema(parsed)


# ============================================================
# TASK 7 — PREPARE MODIFICATIONS
# ============================================================
@task
def prepare_modifications_for_runs(
    user_prompt: str,
    runs_to_modify: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    outputs = []

    for item in runs_to_modify:
        entry = item.get("deployment_entrypoint", "UNKNOWN")
        if entry == "UNKNOWN":
            outputs.append(
                {
                    "filename": None,
                    "status": "skipped",
                    "reason": "UNKNOWN entrypoint",
                }
            )
            continue

        fname = entry.split(":")[0]
        code = item.get("code") or ""
        logs = item.get("logs") or ""

        if not code:
            outputs.append(
                {
                    "filename": fname,
                    "status": "skipped",
                    "reason": "Could not load code",
                }
            )
            continue

        try:
            analysis = analyze_single_flow_for_modification.fn(user_prompt, code, logs)
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
            changes = get_semantic_changes.fn(user_prompt, code)
        except Exception as e:
            outputs.append(
                {
                    "filename": fname,
                    "status": "skipped",
                    "reason": f"Change generation failed: {e}",
                    "analysis": analysis,
                }
            )
            continue

        has_changes = bool(
            changes.get("remove") or changes.get("replace") or changes.get("add")
        )

        if not has_changes:
            outputs.append(
                {
                    "filename": fname,
                    "status": "no_changes",
                    "analysis": analysis,
                    "changes": changes,
                    "original_code": code,
                    "final_code": code,
                }
            )
            continue

        try:
            final_code = apply_json_changes(code, changes)
            validate_python_code(final_code)
        except Exception as e:
            outputs.append(
                {
                    "filename": fname,
                    "status": "skipped",
                    "reason": f"Patch validation failed: {e}",
                    "analysis": analysis,
                    "changes": changes,
                    "original_code": code,
                }
            )
            continue

        outputs.append(
            {
                "filename": fname,
                "status": "ready",
                "analysis": analysis,
                "changes": changes,
                "original_code": code,
                "final_code": final_code,
            }
        )

    return outputs


# ============================================================
# TASK 8 — CREATE PRS
# ============================================================
@task
def create_prs_for_modifications(
    modifications: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
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

        filename = mod["filename"]
        final_code = mod["final_code"]
        analysis = mod.get("analysis", "")
        changes = mod.get("changes", {})

        repo_path = normalize_repo_path(filename)
        branch = f"ai-{uuid.uuid4().hex[:8]}"

        base_ref_url = f"https://api.github.com/repos/{GITHUB_REPO}/git/ref/heads/{BASE_BRANCH}"
        base_ref = github_get(base_ref_url).json()
        sha = base_ref["object"]["sha"]

        create_ref_url = f"https://api.github.com/repos/{GITHUB_REPO}/git/refs"
        github_post(
            create_ref_url,
            {
                "ref": f"refs/heads/{branch}",
                "sha": sha,
            },
        )

        current_info = get_github_file_info(filename)

        update_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{repo_path}"
        github_put(
            update_url,
            {
                "message": f"AI modify {filename}",
                "content": base64.b64encode(final_code.encode("utf-8")).decode("utf-8"),
                "sha": current_info["sha"],
                "branch": branch,
            },
        )

        pr_body = (
            "Automated change from Prefect + Gemini\n\n"
            "## Summary\n"
            f"{analysis}\n\n"
            "## Applied changes\n"
            f"```json\n{safe_json_dumps(changes)}\n```"
        )

        pulls_url = f"https://api.github.com/repos/{GITHUB_REPO}/pulls"
        pr = github_post(
            pulls_url,
            {
                "title": f"AI modification to {filename}",
                "body": pr_body,
                "head": branch,
                "base": BASE_BRANCH,
            },
        )

        created.append(
            {
                "filename": filename,
                "status": "pr_created",
                "pr_created": True,
                "pr_url": pr.json()["html_url"],
            }
        )

    return created


# ============================================================
# MAIN FLOW
# ============================================================
@flow
def summarize_recent_failures_flow(
    user_prompt: str,
    allow_pr: bool = False,
) -> Dict[str, Any]:
    logger = get_run_logger()

    failed_runs = get_failed_runs()
    if not failed_runs:
        logger.info("No failed runs found in the lookback window.")
        return {
            "status": "ok",
            "mode": "none",
            "action": "report",
            "message": "No failed runs found in the lookback window.",
            "selected_flows": [],
            "report": None,
            "modifications": [],
            "pull_requests": [],
        }

    intent = detect_intent(user_prompt)
    mode = intent.get("mode", "single")
    requested_flows = intent.get("flows", [])
    llm_action = intent.get("action", "report")

    if detect_all_flows_request(user_prompt):
        mode = "all"

    action = resolve_action(user_prompt, llm_action)

    explicit_pr_requested = has_explicit_pr_request(user_prompt)
    create_pr_flag = allow_pr and explicit_pr_requested

    logger.info(
        f"Intent detected -> mode={mode}, flows={requested_flows}, "
        f"llm_action={llm_action}, resolved_action={action}, "
        f"explicit_pr_requested={explicit_pr_requested}, allow_pr={allow_pr}, "
        f"create_pr_flag={create_pr_flag}"
    )

    selected_runs = select_target_runs(failed_runs, mode, requested_flows)
    selected_runs = enrich_runs_with_code(selected_runs)

    if not selected_runs:
        logger.info("No runs selected for processing.")
        return {
            "status": "ok",
            "mode": mode,
            "action": action,
            "message": "No runs selected for processing.",
            "selected_flows": [],
            "report": None,
            "modifications": [],
            "pull_requests": [],
        }

    selected_flow_names = [
        r["deployment_entrypoint"].split(":")[0]
        for r in selected_runs
        if r["deployment_entrypoint"] != "UNKNOWN"
    ]

    # --------------------------------------------------------
    # REPORT MODE
    # --------------------------------------------------------
    if action == "report":
        report = build_report(user_prompt, selected_runs)
        logger.info("Report generated successfully.")
        logger.info(report)

        return {
            "status": "ok",
            "mode": mode,
            "action": "report",
            "selected_flows": selected_flow_names,
            "report": report,
            "modifications": [],
            "pull_requests": [],
        }

    # --------------------------------------------------------
    # MODIFY MODE
    # --------------------------------------------------------
    modifications = prepare_modifications_for_runs(user_prompt, selected_runs)
    ready_count = len([m for m in modifications if m.get("status") == "ready"])

    logger.info(f"Prepared modifications. ready_count={ready_count}")

    if not create_pr_flag:
        logger.info("PR creation not explicitly requested. Returning modifications without PR.")
        report = summarize_modifications(modifications)

        return {
            "status": "ok",
            "mode": mode,
            "action": "modify",
            "selected_flows": selected_flow_names,
            "report": report,
            "modifications": modifications,
            "pull_requests": [],
        }

    pr_results = create_prs_for_modifications(modifications)
    logger.info(f"PR results: {pr_results}")

    return {
        "status": "ok",
        "mode": mode,
        "action": "modify",
        "selected_flows": selected_flow_names,
        "report": None,
        "modifications": modifications,
        "pull_requests": pr_results,
    }
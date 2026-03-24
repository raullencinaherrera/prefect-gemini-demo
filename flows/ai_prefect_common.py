from prefect import task
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
from google import genai
from datetime import datetime, timedelta
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
MAX_CODE_CHARS = 80000
MAX_REPORT_CODE_CHARS = 50000
MAX_PRS_DEFAULT = 5


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


def safe_json_dumps(obj: Any) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=False)


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

    candidate = raw[start:end + 1]
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


def b64_decode_text(value: str) -> str:
    try:
        decoded = base64.b64decode(value, validate=True).decode("utf-8")
    except Exception as e:
        raise ValueError(f"Invalid base64 text: {e}")

    bad_chars = []
    for ch in decoded:
        o = ord(ch)
        if o < 32 and ch not in ("\n", "\r", "\t"):
            bad_chars.append(repr(ch))

    if bad_chars:
        raise ValueError(
            f"Decoded base64 contains non-text control characters: {', '.join(bad_chars[:10])}"
        )

    return decoded


def normalize_possible_base64(value: str) -> str:
    """
    If value is already valid base64, keep it.
    If it looks like plain text/code, convert it to base64.
    """
    if not isinstance(value, str):
        raise ValueError("Expected string value")

    try:
        base64.b64decode(value, validate=True)
        return value
    except Exception:
        return base64.b64encode(value.encode("utf-8")).decode("utf-8")


def validate_python_code(code: str) -> None:
    compile(code, "<generated>", "exec")


def extract_filename_from_entrypoint(entrypoint: str) -> Optional[str]:
    if not entrypoint or entrypoint == "UNKNOWN":
        return None
    return entrypoint.split(":")[0]


def available_flow_filenames(runs: List[Dict[str, Any]]) -> List[str]:
    names = []
    for r in runs:
        fname = extract_filename_from_entrypoint(r.get("deployment_entrypoint", "UNKNOWN"))
        if fname:
            names.append(fname)
    return sorted(set(names))


def detect_flow_from_prompt(user_prompt: str, runs: List[Dict[str, Any]]) -> Optional[str]:
    p = user_prompt.lower()
    candidates = available_flow_filenames(runs)

    for fname in candidates:
        if fname.lower() in p:
            return fname

    for fname in candidates:
        base = os.path.splitext(os.path.basename(fname))[0].lower()
        if base and base in p:
            return fname

    return None


def prompt_requests_recent_runs(user_prompt: str) -> bool:
    p = user_prompt.lower()
    terms = [
        "últimos runs",
        "ultimos runs",
        "latest runs",
        "recent runs",
        "runs recientes",
        "últimas ejecuciones",
        "ultimas ejecuciones",
        "last runs",
    ]
    return any(t in p for t in terms)


def prompt_requests_failures(user_prompt: str) -> bool:
    p = user_prompt.lower()
    terms = [
        "fallos",
        "errores",
        "failed",
        "failures",
        "error",
        "failed runs",
        "runs fallidos",
        "ejecuciones fallidas",
    ]
    return any(t in p for t in terms)


def keep_latest_run_per_flow(runs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}

    for r in runs:
        fname = extract_filename_from_entrypoint(r.get("deployment_entrypoint", "UNKNOWN"))
        if not fname:
            continue
        if fname not in latest:
            latest[fname] = r

    return list(latest.values())


def extract_relevant_error(logs: str, max_lines: int = 60) -> str:
    if not logs:
        return ""

    lines = logs.splitlines()

    for i in range(len(lines) - 1, -1, -1):
        line = lines[i]
        if "ERROR" in line or "Exception" in line or "Traceback" in line:
            start = max(0, i - 20)
            end = min(len(lines), i + 40)
            return "\n".join(lines[start:end])

    return "\n".join(lines[-max_lines:])


# ============================================================
# GEMINI HELPERS
# ============================================================
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


# ============================================================
# GITHUB HELPERS
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


def create_pr_for_file(
    filename: str,
    final_code: str,
    analysis: str,
    changes: Dict[str, Any],
) -> Dict[str, Any]:
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

    return {
        "filename": filename,
        "status": "pr_created",
        "pr_created": True,
        "pr_url": pr.json()["html_url"],
    }


# ============================================================
# CODE LOADING
# ============================================================
async def load_code_from_prefect(fname: str) -> Optional[str]:
    path = os.path.join(FLOW_BASE_DIR, fname)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    return None


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

        if entry != "UNKNOWN" and not item.get("code"):
            fname = extract_filename_from_entrypoint(entry)
            if fname:
                try:
                    item["code"] = load_code_best_effort(fname)
                except Exception:
                    item["code"] = ""

        enriched.append(item)

    return enriched


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
                limit=max_runs,
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
                    fname = extract_filename_from_entrypoint(entry)
                    if fname:
                        code = await load_code_from_prefect(fname)

                out.append(
                    {
                        "flow_run_id": str(r.id),
                        "flow_run_name": r.name,
                        "state_name": getattr(r.state, "name", None) if r.state else None,
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
# MODIFICATION HELPERS
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

    normalized_remove = []
    normalized_replace = []
    normalized_add = []

    for i, item in enumerate(remove):
        if not isinstance(item, str):
            raise ValueError(f"'remove[{i}]' must be a string")
        normalized_remove.append(normalize_possible_base64(item))

    for i, item in enumerate(replace):
        if not isinstance(item, dict):
            raise ValueError(f"'replace[{i}]' must be an object")
        if "old_b64" not in item or "new_b64" not in item:
            raise ValueError(f"'replace[{i}]' must contain 'old_b64' and 'new_b64'")
        if not isinstance(item["old_b64"], str) or not isinstance(item["new_b64"], str):
            raise ValueError(f"'replace[{i}].old_b64' and 'replace[{i}].new_b64' must be strings")

        normalized_replace.append(
            {
                "old_b64": normalize_possible_base64(item["old_b64"]),
                "new_b64": normalize_possible_base64(item["new_b64"]),
            }
        )

    for i, item in enumerate(add):
        if not isinstance(item, dict):
            raise ValueError(f"'add[{i}]' must be an object")
        if "after_b64" not in item or "content_b64" not in item:
            raise ValueError(f"'add[{i}]' must contain 'after_b64' and 'content_b64'")
        if not isinstance(item["after_b64"], str) or not isinstance(item["content_b64"], str):
            raise ValueError(f"'add[{i}].after_b64' and 'add[{i}].content_b64' must be strings")

        normalized_add.append(
            {
                "after_b64": normalize_possible_base64(item["after_b64"]),
                "content_b64": normalize_possible_base64(item["content_b64"]),
            }
        )

    return {
        "remove": normalized_remove,
        "replace": normalized_replace,
        "add": normalized_add,
    }


def apply_json_changes(original: str, changes: Dict[str, Any]) -> str:
    result = original

    for block_b64 in changes.get("remove", []):
        block = b64_decode_text(block_b64)
        if not block:
            continue
        if block not in result:
            raise ValueError(f"Remove block not found in file:\n{repr(block)}")
        result = result.replace(block, "", 1)

    for rep in changes.get("replace", []):
        old = b64_decode_text(rep.get("old_b64", ""))
        new = b64_decode_text(rep.get("new_b64", ""))

        if not old:
            raise ValueError("Replace operation has empty 'old_b64'")
        if old not in result:
            raise ValueError(f"Replace target not found in file:\n{repr(old)}")

        result = result.replace(old, new, 1)

    for item in changes.get("add", []):
        after = b64_decode_text(item.get("after_b64", ""))
        content = b64_decode_text(item.get("content_b64", ""))

        if not after:
            raise ValueError("Add operation has empty 'after_b64'")
        if not content:
            raise ValueError("Add operation has empty 'content_b64'")
        if after not in result:
            raise ValueError(f"Add anchor not found in file:\n{repr(after)}")

        result = result.replace(after, after + content, 1)

    return result


@task(cache_policy=NO_CACHE)
def get_semantic_changes(user_prompt: str, code: str, logs: str = "") -> Dict[str, Any]:
    prompt = f"""
You must generate safe source-code edits for a Python file.

User request:
\"\"\"{user_prompt}\"\"\"

Relevant logs:
\"\"\"text
{truncate_text(logs or "", MAX_LOG_CHARS)}
\"\"\"

Full file:
\"\"\"python
{truncate_text(code or "", MAX_CODE_CHARS)}
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

CRITICAL RULES:
- Output JSON only
- No markdown fences
- No explanations
- Do not return the whole file
- Every value in remove must be base64
- Every old_b64/new_b64/after_b64/content_b64 value must be real base64, not plain code
- Do NOT put raw Python code directly into *_b64 fields
- Encode every code fragment as base64 UTF-8
- All old/after values must decode to exact text already present in the file
- Do not invent anchors
- Prefer replace over remove+add when modifying existing code
- If no changes are needed, return empty lists

Example of valid base64 field:
{{
  "replace": [
    {{
      "old_b64": "c29tZSB0ZXh0",
      "new_b64": "b3RoZXIgdGV4dA=="
    }}
  ]
}}
"""
    parsed = gemini_json(prompt, max_attempts=3)
    return validate_change_schema(parsed)


def classify_fixability(analysis: str) -> str:
    text = (analysis or "").lower()

    non_code_terms = [
        "infra-related",
        "config-related",
        "external dependency-related",
        "transient",
        "network",
        "dns",
        "credential",
        "auth",
        "timeout",
        "service unavailable",
        "rate limit",
        "database unavailable",
        "connection refused",
        "name resolution",
    ]

    code_terms = [
        "code-related",
        "logic bug",
        "bug in code",
        "exception handling",
        "syntax",
        "parsing issue",
        "json parsing",
        "missing guard",
        "wrong condition",
        "missing validation",
        "unhandled exception",
        "null check",
        "none check",
        "bad parsing",
        "incorrect logic",
    ]

    if any(t in text for t in non_code_terms):
        return "non_code"

    if any(t in text for t in code_terms):
        return "code"

    return "code"


@task
def prepare_modification_for_single_flow(
    user_prompt: str,
    flow_file: str,
    code: str,
    logs: str,
) -> Dict[str, Any]:
    try:
        analysis = analyze_single_flow_for_modification.fn(user_prompt, code, logs)
    except Exception as e:
        return {
            "filename": flow_file,
            "status": "skipped",
            "reason": f"Analysis failed: {e}",
        }

    try:
        changes = get_semantic_changes.fn(user_prompt, code, logs)
    except Exception as e:
        return {
            "filename": flow_file,
            "status": "skipped",
            "reason": f"Change generation failed: {e}",
            "analysis": analysis,
        }

    has_changes = bool(
        changes.get("remove") or changes.get("replace") or changes.get("add")
    )

    if not has_changes:
        return {
            "filename": flow_file,
            "status": "no_changes",
            "analysis": analysis,
            "changes": changes,
            "original_code": code,
            "final_code": code,
        }

    try:
        final_code = apply_json_changes(code, changes)
        validate_python_code(final_code)
    except Exception as e:
        return {
            "filename": flow_file,
            "status": "skipped",
            "reason": f"Patch validation failed: {e}",
            "analysis": analysis,
            "changes": changes,
            "original_code": code,
        }

    return {
        "filename": flow_file,
        "status": "ready",
        "analysis": analysis,
        "changes": changes,
        "original_code": code,
        "final_code": final_code,
    }

@task(cache_policy=NO_CACHE)
def analyze_and_generate_changes(
    user_prompt: str,
    code: str,
    logs: str = "",
    ) -> Dict[str, Any]:
    prompt = f"""
    You must analyze a failed Python Prefect flow and optionally generate source-code edits.

    User request:
    \"\"\"{user_prompt}\"\"\"

    Relevant logs:
    \"\"\"text
    {truncate_text(logs or "", MAX_LOG_CHARS)}
    \"\"\"

    Full file:
    \"\"\"python
    {truncate_text(code or "", MAX_CODE_CHARS)}
    \"\"\"

    Return ONLY valid JSON with this exact schema:
    {{
    "analysis": "short technical explanation",
    "fixability": "code",
    "changes": {{
    "remove": [],
    "replace": [],
    "add": []
    }}
    }}

    Rules:
    - Output JSON only
    - No markdown fences
    - No explanations outside JSON
    - fixability must be one of: "code", "non_code", "unknown"
    - If the issue is not clearly code-fixable, return empty changes
    - Every value in remove must be base64
    - Every old_b64/new_b64/after_b64/content_b64 value must be real base64, not plain code
    - Do NOT put raw Python code directly into *_b64 fields
    - Encode every code fragment as base64 UTF-8
    - All old/after values must decode to exact text already present in the file
    - Do not invent anchors
    - Prefer replace over remove+add when modifying existing code
    - If no changes are needed, return empty lists
    """
    parsed = gemini_json(prompt, max_attempts=3)

    if not isinstance(parsed, dict):
        raise ValueError("Model response must be a JSON object")

    analysis = parsed.get("analysis", "")
    fixability = parsed.get("fixability", "unknown")
    changes = parsed.get("changes", {"remove": [], "replace": [], "add": []})

    if fixability not in {"code", "non_code", "unknown"}:
        fixability = "unknown"

    validated_changes = validate_change_schema(changes)

    return {
    "analysis": analysis,
    "fixability": fixability,
    "changes": validated_changes,
    }

@task
def prepare_modifications_for_failed_runs(
    user_prompt: str,
    runs_to_modify: List[Dict[str, Any]],
    max_prs: int = MAX_PRS_DEFAULT,
) -> List[Dict[str, Any]]:
    outputs = []
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

        code = item.get("code") or ""
        logs = extract_relevant_error(item.get("logs") or "")

        if not code:
            outputs.append(
                {
                    "filename": fname,
                    "status": "skipped",
                    "reason": "Could not load code",
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
            result = analyze_and_generate_changes.fn(user_prompt, code, logs)
        except Exception as e:
            outputs.append(
                {
                    "filename": fname,
                    "status": "skipped",
                    "reason": f"Analysis/change generation failed: {e}",
                }
            )
            continue

        analysis = result.get("analysis", "")
        fixability = result.get("fixability", "unknown")
        changes = result.get("changes", {"remove": [], "replace": [], "add": []})

        if fixability != "code":
            outputs.append(
                {
                    "filename": fname,
                    "status": "skipped",
                    "reason": f"Not clearly code-fixable ({fixability})",
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
        ready_count += 1

    return outputs


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
                changes=mod.get("changes", {}),
            )
        )

    return created
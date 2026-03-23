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

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
LOOKBACK_MIN = 480
MAX_RUNS = 5
MODEL = "gemini-2.5-flash"
FLOW_BASE_DIR = "/opt/prefect/flows"


# ------------------------------------------------------------
# TASK 1: Collect last N failed runs with REAL code via deployment.entrypoint
# ------------------------------------------------------------
@task(cache_policy=NO_CACHE)
def collect_recent_failed_runs_with_code():

    async def _fetch():
        since = datetime.utcnow() - timedelta(minutes=LOOKBACK_MIN)
        results = []

        async with get_client() as client:
            runs = await client.read_flow_runs(
                flow_run_filter=FlowRunFilter(
                    state=FlowRunFilterState(
                        type=FlowRunFilterStateType(any_=["FAILED"])
                    )
                ),
                sort="START_TIME_DESC",
                limit=MAX_RUNS
            )

            for r in runs:
                if not r.start_time or r.start_time.replace(tzinfo=None) < since:
                    continue

                # --------------------------------------------------
                # 1. Logs
                # --------------------------------------------------
                log_filter = LogFilter(
                    flow_run_id=LogFilterFlowRunId(any_=[str(r.id)])
                )
                logs = await client.read_logs(log_filter=log_filter)

                log_text = "\n".join(
                    f"[{l.timestamp}] {logging.getLevelName(int(l.level)) if l.level else 'LEVEL?'}: {l.message}"
                    for l in logs
                )

                # --------------------------------------------------
                # 2. Deployment → entrypoint → code
                # --------------------------------------------------
                code = "SOURCE CODE NOT FOUND"
                entrypoint = "UNKNOWN"

                if r.deployment_id:
                    deployment = await client.read_deployment(r.deployment_id)
                    entrypoint = deployment.entrypoint  # e.g. device_loader.py:load_devices_batch

                    if entrypoint and ":" in entrypoint:
                        file_name = entrypoint.split(":")[0]
                        code_path = os.path.join(FLOW_BASE_DIR, file_name)

                        if os.path.exists(code_path):
                            with open(code_path) as f:
                                code = f.read()

                results.append({
                    "flow_run_id": str(r.id),
                    "flow_name": r.name,
                    "deployment_entrypoint": entrypoint,
                    "logs": log_text,
                    "code": code,
                })

        return results

    return anyio.run(_fetch)


# ------------------------------------------------------------
# TASK 2: ONE Gemini call — forced code-level analysis
# ------------------------------------------------------------
@task(cache_policy=NO_CACHE)
def analyze_with_gemini(user_prompt: str, failed_runs: list[dict]):
    client = genai.Client()

    context = []

    for i, r in enumerate(failed_runs, start=1):
        context.append(f"""
==============================
FAILED RUN {i}

Flow run name: {r['flow_name']}
Flow run id: {r['flow_run_id']}
Deployment entrypoint: {r['deployment_entrypoint']}

--- SOURCE CODE (AUTHORITATIVE) ---
{r['code']}

--- LOGS (SECONDARY EVIDENCE) ---
{r['logs']}
""")

    prompt = f"""
You are a senior Python + Prefect engineer performing a **STRICT CODE-LEVEL ROOT CAUSE ANALYSIS**.

USER QUESTION:
{user_prompt}

MANDATORY RULES:
- You MUST base your explanation on the SOURCE CODE provided.
- You MUST reference concrete functions, conditions or exceptions in the code.
- You MUST NOT infer behavior that is not visible in the code.
- If the code does not explain the failure, explicitly say so.

YOUR TASK:
1. Identify which FAILED RUN is relevant to the question.
2. Identify the exact code path responsible.
3. Explain WHY the failure happened, citing code.
4. Point to the exact logic that caused the flow to fail.
5. Propose concrete code changes (not operational advice).

CONTEXT:
{''.join(context)}
"""

    resp = client.models.generate_content(
        model=MODEL,
        contents=prompt
    )
    return resp.text


# ------------------------------------------------------------
# MAIN FLOW
# ------------------------------------------------------------
@flow
def summarize_recent_failures_flow(user_prompt: str = ""):
    logger = get_run_logger()

    failed_runs = collect_recent_failed_runs_with_code()

    if not failed_runs:
        logger.info("No hay runs fallidos recientes.")
        return

    analysis = analyze_with_gemini(
        user_prompt=user_prompt,
        failed_runs=failed_runs,
    )

    logger.info("\n### AI CODE-LEVEL ANALYSIS ###\n")
    logger.info(analysis)

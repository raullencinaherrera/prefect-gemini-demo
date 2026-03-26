from prefect import flow, get_run_logger

from ai_prefect_common import (
    LOOKBACK_MIN,
    MAX_RUNS,
    get_recent_runs,
    enrich_runs_with_code,
    build_report,
    extract_filename_from_entrypoint
)

def prompt_requests_recent_runs(user_prompt: str) -> bool:
    p = user_prompt.lower()
    return any(
        kw in p
        for kw in [
            "recent",
            "latest",
            "últimos",
            "ultimos",
            "last runs",
            "recent runs",
        ]
    )


def prompt_requests_failures(user_prompt: str) -> bool:
    p = user_prompt.lower()
    return any(
        kw in p
        for kw in [
            "fail",
            "error",
            "failed",
            "failure",
            "errores",
            "fallos",
        ]
    )

def detect_flow_from_prompt_local(user_prompt: str, runs: list) -> str | None:
    prompt_l = user_prompt.lower()

    candidates = []
    for r in runs:
        fname = extract_filename_from_entrypoint(
            r.get("deployment_entrypoint", "UNKNOWN")
        )
        if fname:
            candidates.append(fname)

    candidates = sorted(set(candidates))

    for fname in candidates:
        if fname.lower() in prompt_l:
            return fname

        base = fname.rsplit(".", 1)[0].lower()
        if base and base in prompt_l:
            return fname

    return None


@flow
def report_failures_flow(
    user_prompt: str,
    lookback_min: int = LOOKBACK_MIN,
    max_runs: int = MAX_RUNS,
) -> dict:
    logger = get_run_logger()

    wants_recent = prompt_requests_recent_runs(user_prompt)
    wants_failures = prompt_requests_failures(user_prompt)

    # Si pide fallos -> solo FAILED
    # Si no pide fallos -> cualquier estado
    state_type = "FAILED" if wants_failures else None

    runs = get_recent_runs(
        lookback_min=lookback_min,
        max_runs=max_runs,
        state_type=state_type,
    )

    if not runs:
        return {
            "status": "ok",
            "message": "No runs found for the requested criteria.",
            "selected_flows": [],
            "report": None,
        }

    requested_flow = detect_flow_from_prompt_local(user_prompt, runs)

    if requested_flow:
        runs = [
            r for r in runs
            if extract_filename_from_entrypoint(
                r.get("deployment_entrypoint", "UNKNOWN")
            ) == requested_flow
        ]

    if not runs:
        return {
            "status": "ok",
            "message": "No matching runs found for the requested flow.",
            "selected_flows": [],
            "report": None,
        }

    # Si pide explícitamente "últimos runs", usamos el conjunto tal como viene.
    # Si no lo pide, igualmente analizamos el conjunto del lookback recibido.
    selected_runs = runs

    selected_runs = enrich_runs_with_code(selected_runs)

    selected_flows = []
    for r in selected_runs:
        fname = extract_filename_from_entrypoint(
            r.get("deployment_entrypoint", "UNKNOWN")
        )
        if fname:
            selected_flows.append(fname)

    report = build_report(user_prompt, selected_runs)

    logger.info("Report generated successfully.")
    logger.info(report)

    return {
        "status": "ok",
        "message": "Report generated successfully.",
        "selected_flows": selected_flows,
        "report": report,
    }
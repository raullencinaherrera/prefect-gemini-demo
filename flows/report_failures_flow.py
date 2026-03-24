from prefect import flow, get_run_logger

from ai_prefect_common import (
    LOOKBACK_MIN,
    MAX_RUNS,
    get_recent_runs,
    enrich_runs_with_code,
    build_report,
    detect_flow_from_prompt,
    extract_filename_from_entrypoint,
    prompt_requests_recent_runs,
    prompt_requests_failures,
)


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

    requested_flow = detect_flow_from_prompt(user_prompt, runs)
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

    # Si el usuario pide explícitamente "últimos runs",
    # dejamos el conjunto limitado por max_runs tal como viene.
    # Si no lo pide, igualmente get_recent_runs ya trae los del lookback,
    # y el análisis se hace sobre ese conjunto.
    if wants_recent:
        selected_runs = runs
    else:
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
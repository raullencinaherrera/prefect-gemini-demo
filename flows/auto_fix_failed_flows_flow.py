from prefect import flow, get_run_logger

from ai_prefect_common import (
    LOOKBACK_MIN,
    MAX_RUNS,
    MAX_PRS_DEFAULT,
    get_failed_runs,
    enrich_runs_with_code,
    prepare_modifications_for_failed_runs,
    create_prs_for_modifications,
    extract_filename_from_entrypoint,
    keep_latest_run_per_flow
)

def dedupe_runs_by_flow_filename(runs: list) -> list:
    seen = set()
    deduped = []

    for r in runs:
        fname = extract_filename_from_entrypoint(
            r.get("deployment_entrypoint", "UNKNOWN")
        )

        if not fname:
            continue

        if fname in seen:
            continue

        seen.add(fname)
        deduped.append(r)

    return deduped

@flow
def auto_fix_failed_flows_flow(
    user_prompt: str = "Analyze recent failed Prefect flows and create pull requests for clearly code-related issues.",
    lookback_min: int = LOOKBACK_MIN,
    max_runs: int = MAX_RUNS,
    max_prs: int = MAX_PRS_DEFAULT,
    create_prs: bool = True,
) -> dict:
    logger = get_run_logger()

    # Traemos una muestra amplia de runs fallidos para no sesgarnos
    # por un flow muy ruidoso.
    failed_runs = get_failed_runs(
        lookback_min=lookback_min,
        max_runs=max_runs * 10,
    )

    if not failed_runs:
        return {
            "status": "ok",
            "message": "No failed runs found in the lookback window.",
            "selected_flows": [],
            "modifications": [],
            "pull_requests": [],
        }

    logger.info(f"Initial failed runs fetched: {len(failed_runs)}")

    # Nos quedamos solo con el último fallo por flow
    failed_runs = keep_latest_run_per_flow(failed_runs)

    # Dedupe defensivo adicional por nombre de fichero
    failed_runs = dedupe_runs_by_flow_filename(failed_runs)

    # Excluimos flows del propio sistema antes de llamar a Gemini
    failed_runs = [
    r for r in failed_runs
    if extract_filename_from_entrypoint(
    r.get("deployment_entrypoint", "UNKNOWN")
    ) not in {
    "auto_fix_failed_flows_flow.py",
    "modify_flow_with_pr_flow.py",
    "report_failures_flow.py",
    "ai_prefect_common.py",
    }
    ]

    # Limitamos número de flows
    failed_runs = failed_runs[:max_runs]

    candidate_flows = sorted({
    extract_filename_from_entrypoint(r.get("deployment_entrypoint", "UNKNOWN"))
    for r in failed_runs
    if extract_filename_from_entrypoint(r.get("deployment_entrypoint", "UNKNOWN"))
    })
    
    logger.info(f"Autofix candidate flows after dedup: {candidate_flows}")

    if not failed_runs:
        return {
            "status": "ok",
            "message": "No failed runs found for the requested flow.",
            "selected_flows": [],
            "modifications": [],
            "pull_requests": [],
        }

    failed_runs = enrich_runs_with_code(failed_runs)

    selected_flows = []
    for r in failed_runs:
        fname = extract_filename_from_entrypoint(
            r.get("deployment_entrypoint", "UNKNOWN")
        )
        if fname:
            selected_flows.append(fname)

    modifications = prepare_modifications_for_failed_runs(
        user_prompt=user_prompt,
        runs_to_modify=failed_runs,
        max_prs=max_prs,
    )

    for mod in modifications:
        logger.info(
            f"MOD RESULT -> flow={mod.get('filename')} "
            f"status={mod.get('status')} "
            f"reason={mod.get('reason')}"
        )

    if not create_prs:
        logger.info("Auto-fix analysis completed without PR creation.")
        return {
            "status": "ok",
            "message": "Auto-fix analysis completed without PR creation.",
            "selected_flows": selected_flows,
            "modifications": modifications,
            "pull_requests": [],
        }

    pr_results = create_prs_for_modifications(modifications)

    created_count = len([p for p in pr_results if p.get("pr_created")])
    ready_count = len([m for m in modifications if m.get("status") == "ready"])
    skipped_count = len([m for m in modifications if m.get("status") == "skipped"])
    no_changes_count = len([m for m in modifications if m.get("status") == "no_changes"])

    logger.info(
        f"Auto-fix finished. selected_flows={len(selected_flows)}, "
        f"ready={ready_count}, no_changes={no_changes_count}, "
        f"skipped={skipped_count}, prs_created={created_count}"
    )

    return {
        "status": "ok",
        "message": "Auto-fix analysis completed.",
        "selected_flows": selected_flows,
        "modifications": modifications,
        "pull_requests": pr_results,
    }
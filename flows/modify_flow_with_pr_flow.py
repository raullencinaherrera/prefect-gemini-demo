from prefect import flow, get_run_logger

from ai_prefect_common import (
    load_code_best_effort,
    prepare_modification_for_single_flow,
    create_pr_for_file,
)


@flow
def modify_flow_with_pr_flow(
    user_prompt: str,
    flow_file: str,
    create_pr: bool = True,
) -> dict:
    logger = get_run_logger()

    code = load_code_best_effort(flow_file)

    modification = prepare_modification_for_single_flow(
        user_prompt=user_prompt,
        flow_file=flow_file,
        code=code,
        logs="",
    )

    if not create_pr:
        logger.info("Modification prepared without PR creation.")
        return {
            "status": "ok",
            "flow_file": flow_file,
            "modification": modification,
            "pull_request": None,
        }

    if modification.get("status") != "ready":
        logger.info(
        f"No PR created. status={modification.get('status')}, "
        f"reason={modification.get('reason')}"
        )
        logger.info(f"Modification payload: {modification}")
        return {
        "status": "ok",
        "flow_file": flow_file,
        "modification": modification,
        "pull_request": None,
        }

    pr_result = create_pr_for_file(
        filename=modification["filename"],
        final_code=modification["final_code"],
        analysis=modification.get("analysis", ""),
        changes=modification.get("changes", {}),
    )

    logger.info(f"PR created: {pr_result.get('pr_url')}")

    return {
        "status": "ok",
        "flow_file": flow_file,
        "modification": modification,
        "pull_request": pr_result,
    }
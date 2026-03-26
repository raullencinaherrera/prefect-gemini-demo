from prefect import flow, get_run_logger

from ai_prefect_common import (
    generate_new_flow_artifact,
    create_pr_for_new_file,
)


@flow
def create_new_flow_with_pr_flow(
    user_prompt: str,
    create_pr: bool = True,
) -> dict:
    logger = get_run_logger()

    artifact = generate_new_flow_artifact(user_prompt)

    logger.info(f"Generated new flow artifact: {artifact.get('filename')}")

    if artifact.get("status") != "ready":
        return {
            "status": "ok",
            "message": artifact.get("reason", "No artifact generated"),
            "artifact": artifact,
            "pull_request": None,
        }

    if not create_pr:
        return {
            "status": "ok",
            "message": "New flow generated without PR creation.",
            "artifact": artifact,
            "pull_request": None,
        }

    pr_result = create_pr_for_new_file(
        filename=artifact["filename"],
        final_code=artifact["final_code"],
        analysis=artifact.get("analysis", ""),
        diff_text=artifact.get("diff", ""),
    )

    logger.info(f"PR result: {pr_result}")

    return {
        "status": "ok",
        "message": "New flow generated and PR created.",
        "artifact": artifact,
        "pull_request": pr_result,
    }
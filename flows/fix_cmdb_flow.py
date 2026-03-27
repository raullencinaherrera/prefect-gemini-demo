import time
import random
from prefect import flow, task, get_run_logger


class CMDBFixFailureError(Exception):
    """Custom exception for failures encountered during CMDB fix operations."""
    pass


@task
def call_boomi_for_cmdb_update(device_name: str) -> dict:
    """
    Simulates calling the Boomi integration for CMDB updates for a single device.
    As per business documentation, all CMDB updates must go through Boomi.
    In a real scenario, this would involve API calls to the Boomi integration platform.
    """
    logger = get_run_logger()
    logger.info(f"Attempting Boomi CMDB update for device: {device_name}...")
    time.sleep(random.uniform(1, 3))  # Simulate network latency and processing time

    # Simulate success or failure for the Boomi integration call
    if random.random() < 0.2:  # 20% chance of failure for simulation purposes
        error_message = random.choice([
            "Boomi integration timed out",
            "Invalid data format sent to Boomi",
            "CMDB API rejected update via Boomi",
            "Boomi connector authentication failed"
        ])
        logger.error(f"Failed to update CMDB for {device_name} via Boomi: {error_message}")
        return {"device": device_name, "status": "FAILED", "reason": error_message}
    else:
        logger.info(f"Successfully initiated Boomi CMDB update for device: {device_name}")
        return {"device": device_name, "status": "SUCCESS", "reason": "CMDB update request successfully sent to Boomi"}


@flow
def fix_cmdb_flow(devices_to_fix: list[str]):
    """
    A corrective Prefect flow designed to remediate 'CMDB validation error: source record not found'
    issues for specified devices. This flow orchestrates the update of CMDB records by
    invoking the Boomi integration for each identified device.

    Args:
        devices_to_fix: A list of device names (strings) that were identified by upstream
                        flows (e.g., `device_loader.py`) as having CMDB-related errors
                        and require remediation.

    Raises:
        CMDBFixFailureError: If any of the CMDB update tasks via Boomi fail, this
                             custom exception is raised to signal a partial or total
                             failure of the remediation process.
    """
    logger = get_run_logger()
    logger.info(f"Starting CMDB corrective flow for the following devices: {devices_to_fix}")

    if not devices_to_fix:
        logger.warning("No devices were provided for CMDB remediation. Flow completing without action.")
        return

    update_tasks = []
    for device in devices_to_fix:
        # Submit each Boomi update call as a separate task, allowing Prefect to run them concurrently.
        task_future = call_boomi_for_cmdb_update.submit(device)
        update_tasks.append(task_future)

    # Wait for all submitted tasks to complete and collect their results.
    # The .result() method will block until each task is finished.
    final_results = [r.result() for r in update_tasks]

    successful_fixes = [r for r in final_results if r["status"] == "SUCCESS"]
    failed_fixes = [r for r in final_results if r["status"] == "FAILED"]

    if successful_fixes:
        logger.info(f"Successfully initiated CMDB fixes for {len(successful_fixes)} devices: "
                    f"{sorted([r['device'] for r in successful_fixes])}")

    if failed_fixes:
        # Log details of failures and raise a custom exception to mark the flow as failed
        logger.error(f"Failed to initiate CMDB fixes for {len(failed_fixes)} devices. "
                     f"Please review the following reasons: {[{'device': r['device'], 'reason': r['reason']} for r in failed_fixes]}")
        raise CMDBFixFailureError(f"CMDB fix failures detected for devices: "
                                  f"{[{'device': r['device'], 'reason': r['reason']} for r in failed_fixes]}")
    else:
        logger.info("All specified CMDB fix operations completed successfully.")

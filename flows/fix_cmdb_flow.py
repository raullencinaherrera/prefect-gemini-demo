import prefect
from prefect import flow, task, get_run_logger
from typing import List, Dict, Any
import random
import asyncio

# --- Tasks for corrective actions ---

@task
async def create_cmdb_source_record(device_name: str) -> Dict[str, Any]:
    """
    Task to simulate the creation of a missing source record for a device in the CMDB.
    This typically involves making an external API call to the CMDB or through
    an integration platform (e.g., Boomi, as sometimes specified by business logic).
    """
    logger = get_run_logger()
    logger.info(f"Attempting to create CMDB source record for device: {device_name}...")
    await asyncio.sleep(random.uniform(1, 3)) # Simulate network latency / processing time

    # Simulate success or failure for the corrective action
    if random.random() < 0.8: # 80% success rate for creation
        logger.info(f"Successfully created CMDB source record for {device_name}.")
        return {"device": device_name, "status": "SUCCESS", "action": "CMDB_CREATE"}
    else:
        error_reason = f"Failed to create CMDB record for {device_name}."
        logger.error(f"Error creating CMDB source record for {device_name}: {error_reason}")
        return {"device": device_name, "status": "FAILED", "action": "CMDB_CREATE", "reason": error_reason}

@task
async def reprocess_device_load(device_name: str) -> Dict[str, Any]:
    """
    Task to simulate re-attempting the device loading process for devices that previously
    failed with generic errors like "Unexpected null response". This mimics a retry
    of the original `load_single_device` logic.
    """
    logger = get_run_logger()
    logger.info(f"Attempting to reprocess device load for: {device_name}...")
    await asyncio.sleep(random.uniform(1, 2)) # Simulate processing time

    # Simulate success or failure for the reprocess action
    if random.random() < 0.9: # 90% success rate for reprocessing
        logger.info(f"Successfully reprocessed device load for {device_name}.")
        return {"device": device_name, "status": "SUCCESS", "action": "REPROCESS_LOAD"}
    else:
        error_reason = f"Failed to reprocess device load for {device_name} after retry."
        logger.error(f"Error reprocessing device load for {device_name}: {error_reason}")
        return {"device": device_name, "status": "FAILED", "action": "REPROCESS_LOAD", "reason": error_reason}

@task
async def log_unhandled_failure(device_name: str, reason: str) -> Dict[str, Any]:
    """
    Task to log failures that could not be automatically remediated by specific tasks,
    indicating that manual review is required.
    """
    logger = get_run_logger()
    logger.warning(f"Unhandled failure reason for device '{device_name}': '{reason}'. Manual review required.")
    return {"device": device_name, "status": "SKIPPED", "action": "MANUAL_REVIEW_REQUIRED", "reason": reason}

# --- Main corrective flow ---

@flow(name="CMDB Remediation Flow", log_prints=True)
async def fix_cmdb_flow(
    failed_device_reports: List[Dict[str, str]] = None
) -> List[Dict[str, Any]]:
    """
    A new corrective Prefect flow designed to remediate device loading failures.
    It processes a list of device failure reports and attempts specific corrective
    actions based on the reason for failure (e.g., CMDB record creation, device load retry).
    Unhandled failures are logged for manual review.

    Args:
        failed_device_reports: A list of dictionaries, where each dictionary
                               contains 'device' (device identifier) and 'reason'
                               (description of the original failure).
                               Example: [
                                 {"device": "router-02", "reason": "CMDB validation error: source record not found"},
                                 {"device": "load-balancer-10", "reason": "Unexpected null response"}
                               ]
    Returns:
        A list of dictionaries detailing the outcome of each corrective action or skipped status.
    """
    logger = get_run_logger()
    if not failed_device_reports:
        logger.info("No failed device reports provided. Exiting corrective flow.")
        return []

    logger.info(f"Initiating CMDB remediation for {len(failed_device_reports)} reported device failures.")

    remediation_futures = []

    for report in failed_device_reports:
        device_name = report.get("device")
        failure_reason = report.get("reason", "").lower()

        if not device_name:
            logger.warning(f"Skipping report due to missing 'device' key: {report}")
            continue

        logger.debug(f"Processing failed device: '{device_name}' with reason: '{failure_reason}'")

        if "cmdb validation error: source record not found" in failure_reason:
            logger.info(f"Identified '{device_name}' for CMDB source record creation.")
            remediation_futures.append(create_cmdb_source_record.submit(device_name))
        elif "unexpected null response" in failure_reason:
            logger.info(f"Identified '{device_name}' for device load reprocessing.")
            remediation_futures.append(reprocess_device_load.submit(device_name))
        else:
            logger.warning(f"Unknown or unhandled failure reason for device '{device_name}': '{report.get('reason', 'unknown reason')}'. Submitting for manual review log.")
            remediation_futures.append(log_unhandled_failure.submit(device_name, report.get("reason", "unknown reason")))

    if not remediation_futures:
        logger.info("No actionable failures found for automated remediation, or all reports were invalid.")
        return []

    logger.info(f"Awaiting completion of {len(remediation_futures)} remediation tasks...")
    # Using asyncio.gather to concurrently wait for all submitted task results
    remediation_results = await asyncio.gather(*remediation_futures)

    # Summarize results
    successful_remediations = [r for r in remediation_results if r and r.get("status") == "SUCCESS"]
    failed_remediations = [r for r in remediation_results if r and r.get("status") == "FAILED"]
    skipped_for_manual_review = [r for r in remediation_results if r and r.get("status") == "SKIPPED"]

    logger.info(f"\n--- CMDB Remediation Summary ---")
    logger.info(f"Total device failures reported: {len(failed_device_reports)}")
    logger.info(f"Total automated remediation tasks initiated: {len(remediation_futures)}")
    logger.info(f"Successful remediations: {len(successful_remediations)}")
    for s in successful_remediations:
        logger.debug(f"  - Device '{s['device']}' via '{s['action']}' completed successfully.")

    logger.info(f"Failed remediations: {len(failed_remediations)}")
    for f in failed_remediations:
        logger.error(f"  - Device '{f['device']}' via '{f.get('action', 'unspecified action')}' failed: {f.get('reason', 'no reason provided')}")
    
    if skipped_for_manual_review:
        logger.warning(f"Devices requiring manual review (unhandled reasons): {len(skipped_for_manual_review)}")
        for s in skipped_for_manual_review:
            logger.warning(f"  - Device '{s['device']}' - Reason: '{s['reason']}'.")

    # Determine overall flow state based on remediation outcomes
    if failed_remediations or skipped_for_manual_review:
        logger.warning("CMDB Remediation Flow completed with some failures or unhandled issues requiring attention.")
    else:
        logger.info("CMDB Remediation Flow completed successfully for all actionable devices.")

    return remediation_results

# Example of how to run the flow locally for testing:
# if __name__ == "__main__":
#     # Sample data reflecting the identified failures from the logs
#     sample_failed_devices = [
#         {"device": "router-02", "reason": "CMDB validation error: source record not found for device router-02"},
#         {"device": "router-23", "reason": "CMDB validation error: source record not found for device router-23"},
#         {"device": "switch-05", "reason": "CMDB validation error: source record not found for device switch-05"},
#         {"device": "load-balancer-10", "reason": "Unexpected null response"},
#         {"device": "firewall-01", "reason": "Permission denied during API call to external service"}, # Example of unhandled error
#         {"device": "server-07", "reason": "Network unreachable error during initial connection"} # Another unhandled
#     ]
#     import asyncio
#     asyncio.run(fix_cmdb_flow(failed_device_reports=sample_failed_devices))

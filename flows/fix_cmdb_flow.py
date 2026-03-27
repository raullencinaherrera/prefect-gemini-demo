import re
from prefect import flow, task, get_run_logger
import random


def _extract_device_from_cmdb_error(error_message: str) -> str | None:
    """
    Extracts the device name from a CMDB validation error message.
    Example: "CMDB validation error: source record not found for device switch-42"
    """
    cmdb_pattern = r"CMDB validation error: .* for device (\S+)"
    match = re.search(cmdb_pattern, error_message)
    if match:
        return match.group(1)
    return None


@task
def parse_and_filter_cmdb_failures(failed_reasons: list[str]) -> list[str]:
    """
    Parses a list of failure reasons to identify CMDB-related errors
    and extract the corresponding unique device names.
    """
    logger = get_run_logger()
    cmdb_devices = []
    logger.info(f"Parsing {len(failed_reasons)} failure reasons...")

    for reason in failed_reasons:
        device_name = _extract_device_from_cmdb_error(reason)
        if device_name:
            cmdb_devices.append(device_name)
            logger.info(f"Identified CMDB failure for device: {device_name} with reason: {reason}")
        else:
            logger.debug(f"Reason not identified as CMDB failure: {reason}")

    if not cmdb_devices:
        logger.info("No CMDB-related failures found to address.")
    else:
        logger.info(f"Found {len(cmdb_devices)} devices with CMDB failures: {', '.join(set(cmdb_devices))}")

    return list(set(cmdb_devices))  # Return unique device names


@task(retries=2, retry_delay_seconds=10)
def update_cmdb_entry(device_name: str) -> dict:
    """
    Simulates updating a CMDB entry for a given device.
    This task would typically integrate with a CMDB system via API.
    """
    logger = get_run_logger()
    logger.info(f"Attempting to update CMDB entry for device: {device_name}")

    # Simulate CMDB update success/failure
    # Introduce a random chance of transient failure for demonstration
    if random.random() < 0.2:  # 20% chance of transient failure
        error_msg = random.choice([
            "CMDB API unreachable",
            "Authentication failed for CMDB system",
            "Concurrent modification error in CMDB",
        ])
        logger.warning(f"Failed to update CMDB for {device_name}: {error_msg}")
        raise Exception(f"Failed to update CMDB for {device_name}: {error_msg}")  # Raise exception for Prefect retries

    logger.info(f"Successfully updated CMDB entry for device: {device_name}")
    return {"device": device_name, "status": "FIXED"}


@flow(name="fix-cmdb-issues", description="Corrective flow to address CMDB validation errors for devices.")
def fix_cmdb_flow(
    failed_reasons: list[str] = None,  # Input from the failed device_loader.py run
):
    """
    Corrective Prefect flow to address CMDB validation errors reported
    by the device_loader.py flow. It identifies devices with CMDB-related failures
    from a list of aggregate failure reasons and attempts to fix their entries
    in the Configuration Management Database.
    """
    logger = get_run_logger()
    logger.info(f"Starting CMDB corrective flow with {len(failed_reasons) if failed_reasons else 0} reported failure reasons.")

    if not failed_reasons:
        logger.info("No failure reasons provided. Exiting corrective flow.")
        return

    # 1. Parse and filter for CMDB-specific failures
    cmdb_devices_to_fix_future = parse_and_filter_cmdb_failures.submit(failed_reasons)

    # Wait for parsing to complete and get the list of unique devices
    devices_for_cmdb_update = cmdb_devices_to_fix_future.result()

    if not devices_for_cmdb_update:
        logger.info("No CMDB-related device failures identified to fix. Exiting.")
        return

    # 2. Attempt to fix CMDB entries for identified devices in parallel
    update_futures = []
    for device in devices_for_cmdb_update:
        update_futures.append(update_cmdb_entry.submit(device))

    # Collect results from CMDB update tasks
    fixed_devices = []
    failed_cmdb_fixes = []
    for future in update_futures:
        try:
            result = future.result()
            fixed_devices.append(result["device"])
        except Exception as e:
            # Catch exceptions from update_cmdb_entry task (e.g., after exhausting retries)
            device_in_error = "unknown-device"
            # Attempt to extract device name from the exception message
            match = re.search(r"Failed to update CMDB for (\S+):", str(e))
            if match:
                device_in_error = match.group(1)
            failed_cmdb_fixes.append(f"{device_in_error}: {str(e)}")
            logger.error(f"Failed to fix CMDB for device {device_in_error} after retries: {str(e)}")

    if fixed_devices:
        logger.info(f"Successfully fixed CMDB entries for devices: {', '.join(fixed_devices)}")

    if failed_cmdb_fixes:
        logger.error(f"Corrective flow completed with errors. Failed CMDB fixes for: {'; '.join(failed_cmdb_fixes)}")
        # Raise an exception if some CMDB fixes failed, so the corrective flow itself reflects failure
        raise Exception(f"Corrective flow completed with errors. Failed CMDB fixes: {'; '.join(failed_cmdb_fixes)}")
    else:
        logger.info("All identified CMDB issues were successfully remediated.")

# Example of how to run this flow (uncomment for local testing or deployment)
# if __name__ == "__main__":
#     # This list would typically be obtained from the logs or state of a failed device_loader.py run.
#     sample_failed_reasons = [
#         'CMDB validation error: source record not found for device switch-42',
#         'Database write error',
#         'Invalid payload received',
#         'CMDB validation error: duplicate entry found for device router-01',
#         'Another generic error',
#         'CMDB validation error: permissions issue for device fw-02',
#     ]
#     fix_cmdb_flow(failed_reasons=sample_failed_reasons)

# To deploy this flow, you might use:
# from prefect import serve
# deployment = fix_cmdb_flow.to_deployment(name="cmdb-corrective-deployment", interval=None)
# if __name__ == "__main__":
#     serve(deployment)

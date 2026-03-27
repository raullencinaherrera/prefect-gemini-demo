from prefect import flow, task, get_run_logger
import random
import asyncio

# --- Tasks ---

@task
async def fix_cmdb_record_task(device_name: str) -> dict:
    """
    Simulates fixing the CMDB record for a given device.
    For demonstration, it has a 90% chance of success.
    """
    logger = get_run_logger()
    logger.info(f"Attempting to remediate CMDB record for device: {device_name}...")
    await asyncio.sleep(random.uniform(0.5, 1.5)) # Simulate work

    # Simulate a high chance of successful remediation for the demo
    success = random.random() > 0.1 # 90% chance of successful remediation
    if success:
        logger.info(f"CMDB record for {device_name} successfully remediated.")
        return {"device": device_name, "status": "REMEDIATED"}
    else:
        reason = "CMDB remediation failed due to an external system error or permission issue."
        logger.error(f"Failed to remediate CMDB for {device_name}: {reason}")
        return {"device": device_name, "status": "REMEDIATION_FAILED", "reason": reason}

@task(retries=2, retry_delay_seconds=5)
async def verify_device_load_task(device_name: str) -> dict:
    """
    Simulates re-loading/verifying the device after CMDB remediation.
    It can still fail if CMDB fix wasn't complete, or due to transient errors (with retries).
    """
    logger = get_run_logger()
    logger.info(f"Attempting to verify/load device {device_name} after CMDB remediation...")
    await asyncio.sleep(random.uniform(0.5, 2.0)) # Simulate work

    # 5% chance CMDB validation still fails (persistent issue even after remediation attempt)
    cmdb_validation_ok = random.random() > 0.05
    if not cmdb_validation_ok:
        reason = "CMDB validation still failing for device (post-remediation review required)."
        logger.error(f"Device {device_name} verification failed: {reason}")
        # This is a persistent failure, so we return a failed status.
        return {"device": device_name, "status": "VERIFICATION_FAILED", "reason": reason}

    # 10% chance of a *transient* error that should trigger retries
    transient_error_candidate = random.random() < 0.1
    if transient_error_candidate:
        reason = random.choice([
            "Network timeout during verification",
            "Temporary API unavailability",
            "Database connection unstable"
        ])
        logger.warning(f"Device {device_name} encountered a transient error during verification: {reason}")
        # Raising an exception here triggers Prefect's retry mechanism.
        raise Exception(f"Transient verification error: {reason}")

    logger.info(f"Device {device_name} successfully verified and loaded.")
    return {"device": device_name, "status": "VERIFIED_OK"}

# --- Flow ---

@flow(name="CMDB Remediation and Device Reload Flow")
async def fix_cmdb_flow(devices_to_fix: list[str]):
    """
    Corrective Prefect flow to remediate CMDB validation issues for a list of devices,
    and then attempt to verify their load status after remediation.
    """
    logger = get_run_logger()
    if not devices_to_fix:
        logger.info("No devices provided for CMDB remediation. Flow completing successfully.")
        return

    logger.info(f"Starting CMDB remediation flow for {len(devices_to_fix)} devices: {devices_to_fix}")

    remediation_futures = [fix_cmdb_record_task.submit(device) for device in devices_to_fix]

    successful_remediated_devices = []
    failed_operation_details = [] # Collects failures from both remediation and verification steps

    # Step 1: Process CMDB remediation results
    for i, future in enumerate(remediation_futures):
        device = devices_to_fix[i]
        result_obj = await future.wait_for_result() # Get PrefectResult object for state inspection

        if result_obj.state.is_completed():
            task_result = result_obj.result() # The actual dict returned by the task
            if task_result["status"] == "REMEDIATED":
                successful_remediated_devices.append(task_result["device"])
            else: # REMEDIATION_FAILED (status returned by task)
                failed_operation_details.append(task_result)
        else: # Remediation task itself failed unexpectedly (e.g., Prefect infrastructure, unhandled exception)
            failed_operation_details.append({
                "device": device,
                "status": "REMEDIATION_TASK_FAILED_UNEXPECTEDLY",
                "reason": str(result_obj.state.result) # The exception message
            })
            logger.error(f"Remediation task for {device} failed unexpectedly: {result_obj.state.result}")


    # Step 2: If any devices were remediated, proceed to verification
    if successful_remediated_devices:
        logger.info(f"Successfully remediated CMDB for {len(successful_remediated_devices)} devices: {successful_remediated_devices}. Proceeding to verification.")
        
        verification_futures = [verify_device_load_task.submit(device) for device in successful_remediated_devices]

        # Process verification results
        for i, future in enumerate(verification_futures):
            device = successful_remediated_devices[i]
            result_obj = await future.wait_for_result()

            if result_obj.state.is_completed():
                task_result = result_obj.result()
                if task_result["status"] != "VERIFIED_OK":
                    failed_operation_details.append(task_result) # Add to failures if not verified OK
            else: # Verification task failed (e.g., after retries exhausted, or other infrastructure issue)
                failed_operation_details.append({
                    "device": device,
                    "status": "VERIFICATION_TASK_FAILED_AFTER_RETRIES",
                    "reason": str(result_obj.state.result) # The exception message
                })
                logger.error(f"Verification task for {device} failed after retries: {result_obj.state.result}")
    else:
        logger.warning("No devices were successfully remediated. Skipping device verification.")

    # Final Summary and Reporting
    successfully_fixed_devices = [
        device for device in devices_to_fix
        if device not in [f["device"] for f in failed_operation_details if "device" in f]
    ]
    
    if successfully_fixed_devices:
        logger.info(f"Overall: Successfully remediated and verified {len(successfully_fixed_devices)} devices: {successfully_fixed_devices}")
    
    if failed_operation_details:
        logger.error(f"Overall: CMDB remediation flow completed with {len(failed_operation_details)} outstanding failures.")
        for failure in failed_operation_details:
            logger.error(f"  Device: {failure.get('device', 'N/A')}, Status: {failure.get('status', 'N/A')}, Reason: {failure.get('reason', 'N/A')}")
        # Following the principle of PR2, the flow completes without raising an exception,
        # allowing for graceful partial success handling and prominent logging of failures.
    else:
        logger.info("Overall: All specified devices were successfully remediated and verified. Flow completed successfully.")

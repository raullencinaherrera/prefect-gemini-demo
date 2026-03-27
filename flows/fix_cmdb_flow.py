from prefect import flow, task, get_run_logger
import random
from typing import List, Dict, Any, Optional

# --- Custom Exceptions ---
class DeviceLoadError(Exception):
    """Base exception for device loading failures."""
    pass

class CMDBError(DeviceLoadError):
    """Exception for CMDB validation failures during device loading."""
    pass

class DeviceLoadingTransientError(DeviceLoadError):
    """Exception for transient device loading errors (e.g., network, database)."""
    pass

class DeviceBatchLoadFailure(Exception):
    """Custom exception for when a batch of device loads fails."""
    pass

# Lista de dispositivos simulados
DEVICES = [
    "router-01", "router-02", "router-23",
    "switch-42", "firewall-05", "server-101",
    "access-point-7", "load-balancer-03", "storage-array-88",
    "sensor-12", "gateway-01", "vm-host-55"
]

# Differentiate between transient and critical errors
TRANSIENT_ERRORS = [
    "Timeout connecting to API",
    "Database write error",
]

CRITICAL_ERRORS = [
    "Invalid payload received",
    "Authentication failure",
    "Unexpected null response",
]

@task(retries=3, retry_delay_seconds=5) # Configure retries for transient issues
def load_single_device(device: str) -> Dict[str, Any]:
    logger = get_run_logger()

    # Caso especial para demo: error de CMDB
    cmdb_fail = random.random() < 0.35
    if cmdb_fail:
        error_msg = f"CMDB validation error: source record not found for device {device}"
        logger.error(f"Device {device} failed: {error_msg}")
        # Raise a specific exception for CMDB failures
        raise CMDBError(error_msg)

    # 20% de otros fallos genéricos
    fail = random.random() < 0.20
    if fail:
        # Differentiate between transient and critical errors
        is_transient = random.random() < 0.5 # 50% chance to be transient
        if is_transient:
            error_reason = random.choice(TRANSIENT_ERRORS)
            logger.warning(f"Device {device} encountered transient failure: {error_reason}. Retrying...")
            raise DeviceLoadingTransientError(error_reason) # Raise transient error
        else:
            error_reason = random.choice(CRITICAL_ERRORS)
            logger.error(f"Device {device} failed: {error_reason}")
            raise DeviceLoadError(error_reason) # Raise general device load error

    logger.info(f"Device {device} loaded OK")
    return {"device": device, "status": "OK", "reason": None}

@flow
def fix_cmdb_flow(): # Renamed from load_devices_batch to fit the new flow name
    logger = get_run_logger()
    task_futures = []

    for dev in DEVICES:
        f = load_single_device.submit(dev)
        task_futures.append(f)

    # Collect all task outcomes, including exceptions, without immediately stopping the flow
    # `f.result(raise_on_failure=False)` will return the result for success, or the exception for failure.
    task_outcomes = [f.result(raise_on_failure=False) for f in task_futures]

    failed_reasons = []
    
    for i, outcome in enumerate(task_outcomes):
        device_name = DEVICES[i] # Get device name corresponding to the future
        if isinstance(outcome, Exception):
            # This task failed; the outcome is the exception object
            failed_reasons.append(str(outcome))
            logger.error(f"Device {device_name} failed: {outcome}")
        else:
            # This task succeeded; the outcome is the dictionary returned by the task
            logger.info(f"Device {device_name} loaded OK.") # The task returns a dict with 'reason': None on success

    # Determine if any tasks failed based on the collected failed_reasons
    if failed_reasons:
        logger.error(f"Flow failed due to device load errors: {failed_reasons}")
        # Raise a specific custom exception for batch failures
        raise DeviceBatchLoadFailure(f"Device load failures: {failed_reasons}")
    else:
        logger.info("All devices loaded successfully.")

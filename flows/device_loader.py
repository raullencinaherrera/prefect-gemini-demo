from prefect import flow, task, get_run_logger
import random
import time

# Lista de dispositivos simulados
DEVICES = [
    "router-01", "router-02", "router-23",
    "switch-05", "switch-11", "switch-42",
    "fw-01", "fw-02",
]

# Posibles errores aleatorios
ERRORS = [
    "Timeout connecting to API",
    "Invalid payload received",
    "Database write error",
    "Authentication failure",
    "Unexpected null response",
]


@task(retries=3, retry_delay_seconds=10)
def load_single_device(device: str):
    logger = get_run_logger()

    # 30% probabilidad de fallo
    fail = random.random() < 0.30

    if fail:
        error = random.choice(ERRORS)
        # If a task fails, raise an exception to allow Prefect's retry mechanism to engage.
        # This message will be logged by Prefect if retries are exhausted.
        logger.warning(f"Attempt to load device {device} failed: {error}")
        raise Exception(f"Device failed to load: {error}")

    logger.info(f"Device {device} loaded OK")
    return {"device": device, "status": "OK", "reason": None}



@flow
def load_devices_batch():
    logger = get_run_logger()
    # Use map for concise parallel submission of tasks to load devices.
    # This simplifies the submission loop and leverages Prefect's parallelism features.
    futures = load_single_device.map(DEVICES)

    results_processed = []
    explicitly_failed_devices = [] # To track devices whose tasks raised exceptions

    # Iterate over futures to collect results or handle exceptions.
    # This ensures the flow waits for all tasks to complete and logs individual task outcomes.
    for i, future in enumerate(futures):
        device_name = DEVICES[i] # Associate future with its device name
        try:
            task_result = future.result() # Will raise Exception if task failed after retries
            results_processed.append(task_result)
        except Exception as e:
            logger.error(f"Device {device_name} task failed definitively after all retries: {e}")
            # Reconstruct the expected failure dictionary for consistency in results_processed.
            # This maintains the original flow's aggregated result structure.
            failed_entry = {"device": device_name, "status": "FAILED", "reason": str(e)}
            results_processed.append(failed_entry)
            explicitly_failed_devices.append(device_name) # Track actual failures

    # If any device failed (either by returning FAILED status or by raising an exception)
    # the flow should fail as per original logic.
    if explicitly_failed_devices: # Check if there were any tasks that definitively failed
        # Collect all devices marked as FAILED from the processed results for the final log and exception.
        failed_devices_for_log = [r["device"] for r in results_processed if r["status"] == "FAILED"]
        logger.error(f"Flow failed due to failing devices: {failed_devices_for_log}")
        raise Exception(f"Device load failures: {failed_devices_for_log}")

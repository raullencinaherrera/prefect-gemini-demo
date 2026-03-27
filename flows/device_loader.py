from prefect import flow, task, get_run_logger
import random

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


@task
def load_single_device(device: str):
    logger = get_run_logger()

    # Caso especial para demo: error de CMDB
    cmdb_fail = random.random() < 0.35
    if cmdb_fail:
        error = f"CMDB validation error: source record not found for device {device}"
        logger.error(f"Device {device} failed: {error}")
        return {"device": device, "status": "FAILED", "reason": error}

    # 20% de otros fallos genéricos
    fail = random.random() < 0.20
    if fail:
        error = random.choice(ERRORS)
        logger.error(f"Device {device} failed: {error}")
        return {"device": device, "status": "FAILED", "reason": error}

    logger.info(f"Device {device} loaded OK")
    return {"device": device, "status": "OK", "reason": None}


@flow
def load_devices_batch():
    logger = get_run_logger()
    results = []

    for dev in DEVICES:
        r = load_single_device.submit(dev)
        results.append(r)

    # Collect all task results. Note that load_single_device returns a dictionary
    # indicating failure status, it does not raise an exception itself for
    # simulated errors, so r.result() will not fail here.
    results = [r.result() for r in results]

    any_failed = any(r["status"] == "FAILED" for r in results)

    if any_failed:
        failed_reasons = [r["reason"] for r in results if r["status"] == "FAILED"]
        logger.error(f"Flow completed with device load errors: {failed_reasons}")
        # The original code raised an exception here, causing the entire Prefect flow run
        # to fail even if some devices loaded successfully. To allow for better analysis
        # of partial successes and to align with robust batch processing patterns,
        # we now log the errors and allow the flow to complete its execution.
        # The flow run will be marked as 'COMPLETED' in Prefect, and the specific
        # device failures will be clearly indicated in the logs.
        # raise Exception(f"Device load failures: {failed_reasons}")
    else:
        logger.info("All devices loaded successfully.")
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

    # Await all task results
    results = [r.result() for r in results]

    any_failed = any(r["status"] == "FAILED" for r in results)

    if any_failed:
        failed_reasons = [r["reason"] for r in results if r["status"] == "FAILED"]
        # Log the failures but allow the flow to complete its execution
        # instead of raising an exception, which would mark the entire flow run as failed.
        logger.error(f"Flow completed with device load errors: {failed_reasons}")
    else:
        logger.info("All devices loaded successfully.")

    # Optionally, you might want to return the results for downstream processing.
    return results
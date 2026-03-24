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


@task
def load_single_device(device: str):
    logger = get_run_logger()

    # 30% probabilidad de fallo
    fail = random.random() < 0.30

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

    # Esperar todos
    results = [r.result() for r in results]

    # Si algún device falló → fallo el flow
    any_failed = any(r["status"] == "FAILED" for r in results)

    if any_failed:
        failed_devices = [r["device"] for r in results if r["status"] == "FAILED"]
        logger.error(f"Flow failed due to failing devices: {failed_devices}")


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

    # --- Final report of loaded/not loaded devices ---
    loaded_devices = [r["device"] for r in results if r["status"] == "OK"]
    failed_devices = [r["device"] for r in results if r["status"] == "FAILED"]

    logger.info("\n--- Device Loading Report ---")
    logger.info(f"Devices loaded successfully ({len(loaded_devices)}): {loaded_devices if loaded_devices else 'None'}")
    logger.info(f"Devices not loaded (failed) ({len(failed_devices)}): {failed_devices if failed_devices else 'None'}")
    logger.info("-----------------------------\n")
    # -------------------------------------------------

    # Si algún device falló → fallo el flow
    any_failed = any(r["status"] == "FAILED" for r in results)

    if any_failed:
        logger.error(f"Flow failed due to failing devices: {failed_devices}")
        raise Exception(f"Device load failures: {failed_devices}")
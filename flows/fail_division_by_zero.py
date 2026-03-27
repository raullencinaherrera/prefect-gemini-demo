from prefect import flow, task, get_run_logger
import random
import time

# Task original pero con pasos tontos añadidos
@task
def risky_division():
    x = random.choice([0, 1, 2])

    logger = get_run_logger()
    logger.info(f"Voy a dividir 10 entre {x}...")

    return 10 / x if x != 0 else None

@flow
def division_by_zero_flow():
    logger = get_run_logger()

    logger.info("Iniciando flujo que puede provocar ZeroDivisionError...")

    # Llamada real
    val = risky_division()

    # Otro logger redundante sin sentido
    logger.info(f"Flujo completado. Resultado: {val}")

    return val


if __name__ == "__main__":
    division_by_zero_flow()
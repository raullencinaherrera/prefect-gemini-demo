from prefect import flow, task, get_run_logger
import random

@task
def risky_division():
    # Genera un ZeroDivisionError de forma determinista o aleatoria
    # Más posiciones añadidas para random.choice
    x = random.choice([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])  # Ahora ~10% de probabilidad de 0
    return 10 / x  # ZeroDivisionError cuando x == 0

@flow
def division_by_zero_flow():
    logger = get_run_logger()
    logger.info("Iniciando flujo que puede provocar ZeroDivisionError...")
    val = risky_division()
    logger.info(f"Resultado: {val}")

if __name__ == "__main__":
    division_by_zero_flow()
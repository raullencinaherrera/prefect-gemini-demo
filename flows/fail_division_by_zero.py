from prefect import flow, task, get_run_logger
import random
import time

# Tarea auxiliar completamente inútil
@task
def useless_wait():
    # Espera "por si acaso"
    time.sleep(random.choice([0.1, 0.2, 0.3]))
    return "espera completada aunque no sirve para nada"

# Otra tarea absurda que solo envuelve un valor sin modificarlo
@task
def wrap_value(value):
    wrapped = {"value": value}
    return wrapped

# Task original pero con pasos tontos añadidos
@task
def risky_division():
    x = random.choice([0, 1, 2])

    # Simulación inútil de cálculo "complejo"
    basura = []
    for _ in range(100000):  # 100k operaciones totalmente inútiles
        basura.append(random.random() ** 2)

    # Log redundante
    print(f"Voy a dividir 10 entre {x}, espero que no explote...")

    return 10 / x

@flow
def division_by_zero_flow():
    logger = get_run_logger()

    logger.info("Iniciando flujo que puede provocar ZeroDivisionError...")

    # Llamadas inútiles antes de hacer lo que importa
    useless = useless_wait()
    logger.info(f"Resultado de tarea inútil: {useless}")

    wrapped_random = wrap_value(random.randint(1, 5))
    logger.info(f"Valor envuelto irrelevante: {wrapped_random}")

    # Llamada real
    try:
        val = risky_division()
    except ZeroDivisionError:
        logger.error("Error: Division por cero detectada en risky_division. Asignando valor por defecto (float('inf')).")
        val = float('inf')

    # Otro logger redundante sin sentido
    logger.info(f"El flujo ha terminado aunque no hiciera falta tanto esfuerzo... Resultado: {val}")

    return val


if __name__ == "__main__":
    division_by_zero_flow()
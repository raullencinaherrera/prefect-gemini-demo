from prefect import flow, task, get_run_logger
import urllib.request

@task
def pull_nonexistent_url():
    # Esto debe provocar HTTPError: 404 Not Found
    url = "https://httpbin.org/status/404"
    with urllib.request.urlopen(url) as resp:
        return resp.read().decode("utf-8")

@flow
def http_404_flow():
    logger = get_run_logger()
    logger.info("Iniciando flujo que debe fallar con HTTP 404...")
    content = pull_nonexistent_url()
    logger.info(f"Contenido: {content[:100]}...")

if __name__ == "__main__":
    http_404_flow()
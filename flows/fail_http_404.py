import urllib.request
import ssl
from prefect import flow, task, get_run_logger

@task
def pull_nonexistent_url():
    # Esto debe provocar HTTPError: 404 Not Found
    url = "https://httpbin.org/status/404"
    # Create an unverified SSL context to bypass certificate verification.
    # This is done to ensure the request can reach the server and trigger
    # the intended HTTP 404 error, as the original failure was due to
    # SSL: CERTIFICATE_VERIFY_FAILED, preventing the HTTP 404 check.
    # In a production environment, proper SSL certificate handling should be used.
    ctx = ssl._create_unverified_context()
    with urllib.request.urlopen(url, context=ctx) as resp:
        return resp.read().decode("utf-8")

@flow
def http_404_flow():
    logger = get_run_logger()
    logger.info("Iniciando flujo que debe fallar con HTTP 404...")
    content = pull_nonexistent_url()
    logger.info(f"Contenido: {content[:100]}...")

if __name__ == "__main__":
    http_404_flow()
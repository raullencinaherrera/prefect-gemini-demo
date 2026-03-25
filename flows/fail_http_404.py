from prefect import flow, task, get_run_logger
import requests # Changed from urllib.request

@task
def pull_nonexistent_url():
    # Esto debe provocar HTTPError: 404 Not Found
    url = "https://httpbin.org/status/404"
    # Using requests library for more robust HTTP handling and to potentially resolve SSL issues.
    # requests often comes with its own CA bundle or integrates better with system certificates,
    # thereby avoiding 'SSL: CERTIFICATE_VERIFY_FAILED' errors seen with urllib.
    response = requests.get(url)
    # Raise an HTTPError for bad responses (4xx or 5xx), which includes 404.
    # This fulfills the original intent of the task failing on a 404 status.
    response.raise_for_status()
    return response.text # requests.Response.text automatically decodes content

@flow
def http_404_flow():
    logger = get_run_logger()
    logger.info("Iniciando flujo que debe fallar con HTTP 404...")
    # This call is expected to raise a requests.exceptions.HTTPError due to the 404 status
    content = pull_nonexistent_url()
    logger.info(f"Contenido: {content[:100]}...")

if __name__ == "__main__":
    http_404_flow()
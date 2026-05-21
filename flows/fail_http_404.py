from prefect import flow, task, get_run_logger
import urllib.request
import ssl

@task
def pull_nonexistent_url():
    # Esto debe provocar HTTPError: 404 Not Found
    url = "https://httpbin.org/status/404"
    # Create an unverified SSL context to bypass certificate verification issues.
    # WARNING: This should be used with caution as it disables important security checks.
    context = ssl.create_unverified_context()
    with urllib.request.urlopen(url, context=context) as resp:
        return resp.read().decode("utf-8")

@flow
def http_404_flow():
    logger = get_run_logger()
    logger.info("Iniciando flujo que debe fallar con HTTP 404...")
    content = pull_nonexistent_url()
    logger.info(f"Contenido: {content[:100]}...")

if __name__ == "__main__":
    http_404_flow()
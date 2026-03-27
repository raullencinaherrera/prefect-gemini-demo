from prefect import flow, task, get_run_logger
import urllib.request
import ssl

@task
def pull_nonexistent_url():
    # Esto debe provocar HTTPError: 404 Not Found
    url = "https://httpbin.org/status/404"
    
    # Create an unverified SSL context to bypass the certificate verification error.
    # This addresses the "SSL: CERTIFICATE_VERIFY_FAILED" error seen in the logs.
    # WARNING: Disabling SSL verification can expose you to security risks if not used carefully.
    # It's appropriate for scenarios like development, testing, or when the environment
    # certificate store is misconfigured and the risk is understood.
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
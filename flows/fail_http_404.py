from prefect import flow, task, get_run_logger
import urllib.request
import urllib.error # Import urllib.error to catch HTTPError specifically

@task
def pull_nonexistent_url():
    # Esto debe provocar HTTPError: 404 Not Found
    # Changed to http to avoid SSL: CERTIFICATE_VERIFY_FAILED
    url = "http://httpbin.org/status/404" 
    try:
        with urllib.request.urlopen(url) as resp:
            return resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        logger = get_run_logger()
        logger.warning(f"Caught expected HTTPError: {e.code} {e.reason}")
        # Return the error response body if available, or just the error message
        return e.read().decode("utf-8") if e.read() else f"Error: {e.code} {e.reason}"

@flow
def http_404_flow():
    logger = get_run_logger()
    logger.info("Iniciando flujo que debe fallar con HTTP 404...")
    content = pull_nonexistent_url()
    logger.info(f"Contenido (o mensaje de error): {content[:100]}...")

if __name__ == "__main__":
    http_404_flow()
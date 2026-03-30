from prefect import flow, task, get_run_logger
import urllib.request
import ssl
from urllib.error import HTTPError

@task
def pull_nonexistent_url():
    # Esto debe provocar HTTPError: 404 Not Found
    url = "https://httpbin.org/status/404"
    # To fix the SSL: CERTIFICATE_VERIFY_FAILED error, use an unverified context.
    # Note: This disables SSL certificate verification and should be used with caution,
    # primarily for testing or specific controlled environments.
    context = ssl._create_unverified_context()
    try:
        with urllib.request.urlopen(url, context=context) as resp:
            # urllib.request.urlopen returns a response object even for 4xx status codes.
            # To ensure the task provokes an HTTPError for 404 as per the comment,
            # we explicitly check the status code and raise an HTTPError.
            if 400 <= resp.getcode() < 600:
                raise HTTPError(url, resp.getcode(), resp.reason, resp.headers, resp)
            return resp.read().decode("utf-8")
    except HTTPError as e:
        # Re-raise the HTTPError that we either caught or explicitly raised
        raise
    except Exception as e:
        # Catch any other unexpected errors, for example, network issues
        raise urllib.error.URLError(f"Failed to open URL {url}: {e}")

@flow
def http_404_flow():
    logger = get_run_logger()
    logger.info("Iniciando flujo que debe fallar con HTTP 404...")
    # The flow is designed to fail due to a 404. The task will now raise HTTPError for this.
    content = pull_nonexistent_url()
    logger.info(f"Contenido: {content[:100]}...")

if __name__ == "__main__":
    http_404_flow()
import time
from prefect import flow, task
from prefect.logging import get_run_logger

@task
def simulate_url_check(url: str, delay_seconds: float = 2.0) -> bool:
    """
    Simulates checking the availability of a URL.
    For this dummy flow, it always reports success after a delay.
    """
    logger = get_run_logger()
    logger.info(f"Simulating check for URL: {url}...")
    time.sleep(delay_seconds) # Simulate network latency or processing time

    # In a real scenario, you'd typically use a library like 'requests' here:
    # import requests
    # try:
    #     response = requests.get(url, timeout=5) # 5-second timeout
    #     is_available = response.status_code == 200
    # except requests.exceptions.RequestException as e:
    #     logger.warning(f"Failed to reach URL '{url}': {e}")
    #     is_available = False

    is_available = True # For our dummy simulation, the URL is always 'available'

    if is_available:
        logger.info(f"SIMULATED: URL '{url}' is available! (Our dummy service confirms.)")
    else:
        logger.warning(f"SIMULATED: URL '{url}' is NOT available. This is unexpected for a dummy URL!")

    return is_available

@flow(name="Dummy URL Availability Checker")
def url_availability_flow(
    target_url: str = "http://www.never-gonna-give-you-up.com/rickroll-prefect-engineers"
) -> None:
    """
    A Prefect flow to simulate checking the availability of a specified URL.
    Uses a funny, dummy URL by default for demonstration purposes.
    """
    logger = get_run_logger()
    logger.info(f"Starting URL availability check flow for: {target_url}")

    availability_status = simulate_url_check(target_url)

    if availability_status:
        logger.info(f"Flow finished: '{target_url}' was SIMULATED as AVAILABLE. Everything's coming up Prefect!")
    else:
        logger.error(f"Flow finished: '{target_url}' was SIMULATED as UNAVAILABLE. Check your dummy URL settings!")

if __name__ == "__main__":
    # To run this flow, simply execute the file:
    # python url_availability_checker.py
    #
    # You can also pass a different URL if you wish:
    # url_availability_flow(target_url="https://www.prefect.io")
    url_availability_flow()

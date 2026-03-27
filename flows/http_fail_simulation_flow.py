import time
import random
import logging
from typing import Optional, Literal
import requests # Used for realistic exception types

from prefect import flow, task, get_run_logger


# Define a simple mock response class to simulate requests.Response
class MockResponse:
    """
    A mock object to simulate a `requests.Response` object for HTTP status failures.
    """
    def __init__(self, status_code: int, url: str, content: str = ""):
        self.status_code = status_code
        self.url = url
        self.content = content.encode('utf-8')
        self.text = content
        self.ok = 200 <= status_code < 300

    def raise_for_status(self):
        """
        Simulates `requests.Response.raise_for_status()`.
        Raises an HTTPError if the status code indicates an error (>= 400).
        """
        if not self.ok:
            raise requests.exceptions.HTTPError(f"Simulated HTTP Error: {self.status_code} for {self.url}", response=self)

    def json(self):
        """
        Simulates `requests.Response.json()`.
        """
        return {"simulated_response": self.text, "status_code": self.status_code, "url": self.url}


@task(retries=0) # We're simulating failures, so retries on the task itself might mask the intended failure.
def simulate_http_call(
    url: str,
    failure_type: Literal['none', 'connection', 'timeout', 'client_4xx', 'server_5xx', 'random'] = 'none',
    probability: float = 0.5,
    specific_status_code: Optional[int] = None,
    delay_seconds: float = 0.1,
) -> MockResponse:
    """
    Simulates an HTTP call, potentially introducing various types of failures.

    Args:
        url: The URL to simulate calling.
        failure_type: The type of failure to simulate.
                      - 'none': Simulate success (200 OK).
                      - 'connection': Raise a requests.exceptions.ConnectionError.
                      - 'timeout': Raise a requests.exceptions.Timeout.
                      - 'client_4xx': Return a 4xx status code (e.g., 400, 404).
                      - 'server_5xx': Return a 5xx status code (e.g., 500, 503).
                      - 'random': Randomly pick a failure type based on 'probability'.
        probability: If failure_type is 'random', this is the chance (0.0 to 1.0)
                     of a failure occurring.
        specific_status_code: If 'client_4xx' or 'server_5xx', use this specific
                              HTTP status code. Otherwise, a common one is chosen.
        delay_seconds: Simulate network latency or processing time.

    Returns:
        A MockResponse object representing the simulated HTTP response.

    Raises:
        requests.exceptions.ConnectionError: If failure_type is 'connection'.
        requests.exceptions.Timeout: If failure_type is 'timeout'.
    """
    logger = get_run_logger()
    logger.info(f"Simulating HTTP call to {url} with failure_type='{failure_type}'...")
    time.sleep(delay_seconds)

    current_failure_type = failure_type
    if failure_type == 'random':
        if random.random() < probability:
            # Pick a random failure type from the non-random/non-none ones
            possible_failures = ['connection', 'timeout', 'client_4xx', 'server_5xx']
            current_failure_type = random.choice(possible_failures)
            logger.warning(f"Randomly selected failure type: '{current_failure_type}' for {url}")
        else:
            current_failure_type = 'none'
            logger.info(f"Randomly selected: No failure for {url}.")

    if current_failure_type == 'connection':
        logger.error(f"Simulating ConnectionError for {url}")
        raise requests.exceptions.ConnectionError(f"Simulated connection error to {url}")
    elif current_failure_type == 'timeout':
        logger.error(f"Simulating Timeout for {url}")
        raise requests.exceptions.Timeout(f"Simulated timeout error for {url}")
    elif current_failure_type == 'client_4xx':
        status_code = specific_status_code if specific_status_code and 400 <= specific_status_code < 500 else random.choice([400, 401, 403, 404])
        logger.warning(f"Simulating Client Error (HTTP {status_code}) for {url}")
        return MockResponse(status_code, url, f"Client Error: {status_code}")
    elif current_failure_type == 'server_5xx':
        status_code = specific_status_code if specific_status_code and 500 <= specific_status_code < 600 else random.choice([500, 502, 503, 504])
        logger.warning(f"Simulating Server Error (HTTP {status_code}) for {url}")
        return MockResponse(status_code, url, f"Server Error: {status_code}")
    else: # 'none'
        logger.info(f"Simulating successful response (HTTP 200) for {url}")
        return MockResponse(200, url, "Success")


@flow(
    name="HTTP Failure Simulation Flow",
    description="A flow to simulate various HTTP failures for testing and observability."
)
def http_fail_simulation_flow(
    target_url: str = "https://api.example.com/data",
    num_simulations: int = 5,
    failure_scenario: Literal['none', 'connection', 'timeout', 'client_4xx', 'server_5xx', 'random_mix'] = 'random_mix',
    random_failure_probability: float = 0.6,
    specific_http_status: Optional[int] = None,
):
    """
    Main flow to orchestrate HTTP failure simulations.

    Args:
        target_url: The base URL to use for simulations.
        num_simulations: How many simulation attempts to make within this flow run.
        failure_scenario: Defines the overall failure behavior for the sequence of simulations.
                          - 'none': All simulations will succeed.
                          - 'connection': All will simulate connection errors.
                          - 'timeout': All will simulate timeout errors.
                          - 'client_4xx': All will simulate 4xx HTTP errors.
                          - 'server_5xx': All will simulate 5xx HTTP errors.
                          - 'random_mix': Each simulation will have a 'random' failure type
                                          based on `random_failure_probability`.
        random_failure_probability: If `failure_scenario` is 'random_mix', this is the
                                    probability (0.0 to 1.0) of any given simulation attempt failing.
        specific_http_status: If simulating 4xx or 5xx errors, use this specific status code.
    """
    logger = get_run_logger()
    logger.info(f"Starting HTTP Failure Simulation Flow for '{target_url}' with {num_simulations} simulations.")
    logger.info(f"Scenario: '{failure_scenario}' | Random Probability: {random_failure_probability} | Specific Status: {specific_http_status}")

    results = []
    for i in range(num_simulations):
        logger.info(f"--- Simulation attempt {i + 1}/{num_simulations} ---")
        
        current_failure_type = failure_scenario
        current_probability = 0.0 # Not relevant unless current_failure_type becomes 'random'
        current_status_code = specific_http_status

        if failure_scenario == 'random_mix':
            current_failure_type = 'random'
            current_probability = random_failure_probability

        try:
            # We explicitly handle the potential exceptions here to keep the flow running
            # for all `num_simulations` attempts, and to collect results.
            # The Prefect UI will still show the task as failed if it raises an exception.
            response = simulate_http_call(
                url=f"{target_url}/{i+1}", # Make URLs slightly unique for logging
                failure_type=current_failure_type,
                probability=current_probability,
                specific_status_code=current_status_code,
                delay_seconds=random.uniform(0.05, 0.5) # Vary delay a bit for realism
            )
            logger.info(f"Simulation {i+1} Result: Success (Status: {response.status_code})")
            results.append({"attempt": i + 1, "status": "success", "status_code": response.status_code, "url": response.url})
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            logger.error(f"Simulation {i+1} Result: Failure (Type: {type(e).__name__}, Message: {e})")
            results.append({"attempt": i + 1, "status": "failed", "error_type": type(e).__name__, "message": str(e), "url": f"{target_url}/{i+1}"})
        except requests.exceptions.HTTPError as e:
            logger.error(f"Simulation {i+1} Result: Failure (Type: HTTPError, Status: {e.response.status_code}, Message: {e})")
            results.append({"attempt": i + 1, "status": "failed", "error_type": "HTTPError", "status_code": e.response.status_code, "message": str(e), "url": f"{target_url}/{i+1}"})
        except Exception as e:
            # Catch any other unexpected errors
            logger.error(f"Simulation {i+1} Result: Unexpected Failure (Type: {type(e).__name__}, Message: {e})")
            results.append({"attempt": i + 1, "status": "failed", "error_type": type(e).__name__, "message": str(e), "url": f"{target_url}/{i+1}"})

    successful_count = sum(1 for r in results if r["status"] == "success")
    failed_count = len(results) - successful_count
    logger.info(f"Flow finished. Total simulations: {num_simulations}. Successful: {successful_count}, Failed: {failed_count}")
    return results


if __name__ == "__main__":
    # Example usage (uncomment to run specific scenarios locally):

    # 1. Simulate 3 successful calls
    # http_fail_simulation_flow(num_simulations=3, failure_scenario='none')

    # 2. Simulate 5 connection errors
    # http_fail_simulation_flow(num_simulations=5, failure_scenario='connection')

    # 3. Simulate 4 timeout errors
    # http_fail_simulation_flow(num_simulations=4, failure_scenario='timeout')

    # 4. Simulate 6 calls with a random mix of failures (70% chance of failure per call)
    http_fail_simulation_flow(num_simulations=6, failure_scenario='random_mix', random_failure_probability=0.7)

    # 5. Simulate 5 calls, all returning 404 Not Found
    # http_fail_simulation_flow(num_simulations=5, failure_scenario='client_4xx', specific_http_status=404)

    # 6. Simulate 5 calls, all returning 503 Service Unavailable
    # http_fail_simulation_flow(num_simulations=5, failure_scenario='server_5xx', specific_http_status=503)

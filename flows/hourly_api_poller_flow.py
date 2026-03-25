import requests
import asyncio
from prefect import flow, task, get_run_logger

# Define a constant for the API URL. In a real-world scenario,
# this might come from an environment variable or a configuration management system.
# Using a reliable public API for demonstration purposes.
API_URL = "https://jsonplaceholder.typicode.com/todos/1"

@task(retries=3, retry_delay_seconds=10)
async def call_api_and_log_response(api_url: str):
    """
    Calls a specified API endpoint, logs the full response, and retries on failure.

    Args:
        api_url: The URL of the API endpoint to call.

    Returns:
        The JSON response from the API if successful, None otherwise.
    
    Raises:
        requests.exceptions.RequestException: If an error occurs during the API call,
                                            to trigger Prefect's retry mechanism.
    """
    logger = get_run_logger()
    logger.info(f"Attempting to call API: {api_url}")

    try:
        # requests is a synchronous library. Prefect handles running it in a thread pool
        # when called from an async task, preventing blocking of the event loop.
        # Set a timeout for the request to prevent hanging indefinitely.
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

        logger.info(f"API call successful for {api_url}.")
        logger.info(f"Response Status Code: {response.status_code}")
        logger.info(f"Response Body: {response.text}")

        return response.json() # Return parsed JSON if successful

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error occurred while calling {api_url}: {e}")
        raise # Re-raise to trigger Prefect's retry mechanism

    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error occurred while calling {api_url}: {e}")
        raise # Re-raise to trigger Prefect's retry mechanism

    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout error occurred while calling {api_url}: {e}")
        raise # Re-raise to trigger Prefect's retry mechanism

    except requests.exceptions.RequestException as e:
        # Catch any other requests-related exceptions
        logger.error(f"An unexpected request error occurred while calling {api_url}: {e}")
        raise # Re-raise to trigger Prefect's retry mechanism

    except Exception as e:
        # Catch any other unexpected errors
        logger.error(f"An unexpected error occurred: {e}")
        raise


@flow(name="Hourly API Poller Flow")
async def hourly_api_check_flow():
    """
    This flow calls a specified API endpoint, logs its response, and handles retries.

    To schedule this flow to run every hour, you would typically create a Prefect
    deployment for it. For example, using the Prefect CLI:

    1. Save this file as `hourly_api_poller_flow.py`.
    2. Build a deployment:
       `prefect deployment build hourly_api_poller_flow.py:hourly_api_check_flow -n "Hourly API Checker Deployment" --schedule "interval:1h"`
    3. Apply the deployment:
       `prefect deployment apply hourly_api_check_flow-deployment.yaml`

    This will register a deployment that runs this flow every hour.
    """
    logger = get_run_logger()
    logger.info("Hourly API check flow started.")

    # Call the API task. Using .submit() allows Prefect to manage the task's state
    # asynchronously within the flow. For a single task, a direct await call
    # (`await call_api_and_log_response(api_url=API_URL)`) would also work.
    api_response_future = await call_api_and_log_response.submit(api_url=API_URL)

    # You can retrieve the result if needed, though for logging purposes, the task handles it.
    # api_response = await api_response_future.wait_for_result()

    logger.info("Hourly API check flow finished.")

# Optional: If you want to run this flow directly for local testing without a deployment,
# uncomment the following lines. Note that scheduling is handled by deployments.
# if __name__ == "__main__":
#     asyncio.run(hourly_api_check_flow())

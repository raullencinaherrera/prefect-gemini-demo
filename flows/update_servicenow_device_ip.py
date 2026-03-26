import os
import random
import requests
from prefect import flow, task, get_run_logger
from typing import Dict, Any

# --- Configuration ---
# In a production environment, it is highly recommended to use Prefect Blocks
# (e.g., a Secret block for API_KEY, or a custom JSON block for API_URL) for
# sensitive information and dynamic configuration.
# For this example, we use environment variables. Please set them before running:
# export BOOMI_API_BASE_URL="http://localhost:8080/boomi-integration-endpoint" # Replace with your actual Boomi API base URL
# export BOOMI_API_KEY="your_boomi_api_key_here" # Replace with your actual Boomi API key

BOOMI_API_BASE_URL = os.getenv("BOOMI_API_BASE_URL", "http://localhost:8080/boomi-integration-endpoint")
BOOMI_API_KEY = os.getenv("BOOMI_API_KEY", "dummy-api-key") # A dummy value for local testing, replace with actual for real calls
DEVICE_ID = "sw_01"
FIELD_TO_CHANGE = "ip_address"

# --- Tasks ---

@task(
    name="Generate Random IP Address",
    description="Generates a random IPv4 address in a common private range (192.168.X.Y)."
)
def generate_random_ip_address() -> str:
    """
    Generates a random IPv4 address in a private range (e.g., 192.168.X.Y).
    The last octet is between 1 and 254 to avoid network and broadcast addresses.
    """
    logger = get_run_logger()
    # Using 192.168.X.Y range for a common private network simulation
    octets = [192, 168, random.randint(0, 255), random.randint(1, 254)]
    random_ip = ".".join(map(str, octets))
    logger.info(f"Generated random IP address: {random_ip}")
    return random_ip

@task(
    name="Call Boomi Integration",
    description="Simulates calling a Boomi integration to update a ServiceNow device field.",
    retries=3, # Retry up to 3 times on failure
    retry_delay_seconds=10 # Wait 10 seconds between retries
)
def call_boomi_integration(
    device_id: str,
    field_name: str,
    new_value: str,
    boomi_url: str,
    api_key: str
) -> Dict[str, Any]:
    """
    Calls a hypothetical Boomi integration endpoint to update a ServiceNow device field.

    This task makes an HTTP POST request. The exact URL path and payload structure
    must conform to your specific Boomi process API setup for ServiceNow integration.

    Args:
        device_id: The ID of the device to update (e.g., 'sw_01').
        field_name: The name of the field to update (e.g., 'ip_address').
        new_value: The new value for the field.
        boomi_url: The base URL for the Boomi integration endpoint.
        api_key: The API key for Boomi authentication (e.g., a Bearer token).

    Returns:
        A dictionary representing the JSON response from the Boomi API.
        If the Boomi API returns a successful status code but no valid JSON,
        it will return a dictionary with 'status_code' and 'response_text'.

    Raises:
        requests.exceptions.RequestException: If the HTTP request fails due to
                                              network errors, timeouts, or bad HTTP status codes.
        ValueError: If the JSON response cannot be parsed and the status code is not 2xx.
    """
    logger = get_run_logger()

    # This payload structure is a hypothetical example. You must adapt it
    # to precisely match the input parameters expected by your Boomi process API.
    payload = {
        "action": "updateDeviceField",
        "targetSystem": "ServiceNow",
        "entityType": "device",
        "entityId": device_id,
        "fieldsToUpdate": {
            field_name: new_value
        }
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}" # Common authentication scheme for APIs
    }

    # Construct the full Boomi endpoint URL.
    # Replace 'servicenow_device_update' with the actual path exposed by your Boomi process.
    full_boomi_endpoint = f"{boomi_url}/servicenow_device_update"

    logger.info(f"Attempting to call Boomi integration at: {full_boomi_endpoint}")
    logger.info(f"Request Payload: {payload}")

    try:
        response = requests.post(full_boomi_endpoint, json=payload, headers=headers, timeout=30)
        response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)

        try:
            response_json = response.json()
            logger.info(f"Successfully called Boomi integration. Response: {response_json}")
            return response_json
        except ValueError as json_err:
            # Handle cases where Boomi API returns a non-JSON but successful response
            if 200 <= response.status_code < 300:
                logger.warning(
                    f"Boomi API returned a non-JSON success response (Status: {response.status_code}). "
                    f"Returning status and raw text. Error: {json_err}"
                )
                return {"status_code": response.status_code, "response_text": response.text}
            else:
                # If it's not a success and not JSON, raise the error
                logger.error(f"Failed to parse JSON response from Boomi (Status: {response.status_code}): {json_err} - Response text: {response.text}")
                raise
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred while calling Boomi (Status: {response.status_code if 'response' in locals() else 'N/A'}): {http_err} - Response: {response.text if 'response' in locals() else 'N/A'}")
        raise
    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"Connection error occurred while calling Boomi: {conn_err}")
        raise
    except requests.exceptions.Timeout as timeout_err:
        logger.error(f"Timeout error occurred while calling Boomi after 30 seconds: {timeout_err}")
        raise
    except requests.exceptions.RequestException as req_err:
        logger.error(f"An unexpected request error occurred while calling Boomi: {req_err}")
        raise

# --- Flow ---

@flow(
    name="Update ServiceNow Device IP via Boomi",
    description=(
        "Prefect flow to update the 'ip_address' field of a specified ServiceNow device "
        "('sw_01') via Boomi integration with a new random IP address."
    )
)
def update_servicenow_device_ip_via_boomi():
    """
    Prefect flow to update the 'ip_address' field of device 'sw_01' in ServiceNow
    by calling a Boomi integration, with a newly generated random IP address.

    This flow orchestrates the following steps:
    1. Generates a new random private IPv4 address using the `generate_random_ip_address` task.
    2. Calls a hypothetical Boomi API endpoint to propagate this change to ServiceNow
       using the `call_boomi_integration` task.
    """
    logger = get_run_logger()
    logger.info(f"Starting flow to update device '{DEVICE_ID}'s '{FIELD_TO_CHANGE}' via Boomi.")

    # 1. Generate a new random IP address by submitting the task
    new_ip_address_future = generate_random_ip_address.submit()
    new_ip_address_result = new_ip_address_future.result()

    # 2. Call Boomi integration to update ServiceNow by submitting the task
    try:
        boomi_response_future = call_boomi_integration.submit(
            device_id=DEVICE_ID,
            field_name=FIELD_TO_CHANGE,
            new_value=new_ip_address_result,
            boomi_url=BOOMI_API_BASE_URL,
            api_key=BOOMI_API_KEY
        )
        boomi_response = boomi_response_future.result()
        logger.info(f"Flow completed successfully. Boomi integration response: {boomi_response}")
    except Exception as e:
        logger.error(f"Flow failed due to an error during Boomi integration call: {e}")
        # Re-raise the exception to ensure the flow run is marked as 'Failed' in Prefect
        raise

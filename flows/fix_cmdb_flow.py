import os
import json
import requests
from prefect import flow, task, get_run_logger
from prefect.engine.states import Failed
from requests.exceptions import Timeout, ConnectionError, RequestException

# Constants
BOOMI_ENDPOINT = "https://boomi.example.internal/api/cmdb/update"
SOURCE_SYSTEM = "prefect_ai_remediation"
API_KEY_ENV_VAR = "BOOMI_API_KEY"

# Define a custom retry condition for the task
def retry_on_timeout_or_connection_error(task_run, state):
    """
    Custom retry condition to retry only on network-related transient errors
    like Timeout or ConnectionError, but not on HTTP errors (e.g., 4xx, 5xx).
    """
    if isinstance(state, Failed) and state.result.exception:
        exc = state.result.exception
        # Retry only if the exception is a timeout or a connection error
        if isinstance(exc, (Timeout, ConnectionError)):
            logger = get_run_logger()
            logger.warning(
                f"Retrying task '{task_run.name}' due to transient error: {type(exc).__name__}. "
                f"Attempt {task_run.run_count + 1}/{task_run.retries + 1}"
            )
            return True
        # For other RequestExceptions (e.g., HTTPError for non-200 status), do not retry
        elif isinstance(exc, RequestException):
            logger = get_run_logger()
            status_code = getattr(exc.response, 'status_code', 'N/A') if hasattr(exc, 'response') else 'N/A'
            logger.error(
                f"Not retrying task '{task_run.name}' due to permanent HTTP error: {type(exc).__name__}: {str(exc)}. "
                f"Status: {status_code}"
            )
            return False
        # For any other unexpected exceptions, do not retry as per strict interpretation.
        logger = get_run_logger()
        logger.error(f"Not retrying task '{task_run.name}' due to unhandled exception type: {type(exc).__name__}")
        return False
    return False # Don't retry if not failed or no exception

@task(retries=3, retry_delay_seconds=10, retry_condition=retry_on_timeout_or_connection_error)
def update_cmdb_via_boomi(device_id: str, field_name: str, field_value: str):
    logger = get_run_logger()

    # 1. Get API Key from environment variable
    boomi_api_key = os.getenv(API_KEY_ENV_VAR)
    if not boomi_api_key:
        raise ValueError(f"Environment variable '{API_KEY_ENV_VAR}' not set. Cannot proceed with CMDB update.")

    # 2. Construct Headers
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": boomi_api_key
    }

    # 3. Construct Payload
    payload = {
        "device_id": device_id,
        "field_name": field_name,
        "field_value": field_value,
        "source": SOURCE_SYSTEM
    }

    # 4. Log outgoing payload (excluding secrets)
    # The payload itself does not contain secrets, only the header does.
    logger.info(f"Attempting to update CMDB for device '{device_id}' via Boomi.")
    logger.info(f"Outgoing Boomi payload: {json.dumps(payload)}")

    try:
        # 5. Make POST request with a timeout
        response = requests.post(BOOMI_ENDPOINT, headers=headers, json=payload, timeout=30)
        
        # Raise an exception for bad status codes (4xx or 5xx)
        response.raise_for_status()

        # 6. Parse JSON response and log remediation result
        try:
            response_json = response.json()
            logger.info(f"CMDB update successful for device '{device_id}'. Boomi response: {json.dumps(response_json)}")
            return {"status": "SUCCESS", "response": response_json}
        except json.JSONDecodeError:
            logger.error(f"CMDB update for device '{device_id}' succeeded but response is not valid JSON. Status Code: {response.status_code}, Response Body: {response.text}")
            raise Exception(f"Invalid JSON response from Boomi for device '{device_id}'")

    except Timeout as e:
        # This exception type will trigger a retry due to retry_condition
        logger.error(f"Timeout connecting to Boomi for device '{device_id}': {e}")
        raise # Re-raise to be caught by Prefect's retry mechanism
    except ConnectionError as e:
        # This exception type will trigger a retry due to retry_condition
        logger.error(f"Connection error to Boomi for device '{device_id}': {e}")
        raise # Re-raise to be caught by Prefect's retry mechanism
    except requests.exceptions.HTTPError as e:
        # This exception type will NOT trigger a retry due to retry_condition
        logger.error(f"Boomi API returned an error for device '{device_id}'. Status Code: {e.response.status_code}, Response: {e.response.text}")
        raise Exception(f"Boomi API error for device '{device_id}': Status {e.response.status_code}, Message: {e.response.text}")
    except RequestException as e:
        # Catch any other requests-related exceptions not specifically handled
        logger.error(f"An unexpected request error occurred for device '{device_id}': {e}")
        raise # Re-raise to mark the task as failed
    except Exception as e:
        # Catch any other unexpected errors
        logger.error(f"An unexpected error occurred during CMDB update for device '{device_id}': {e}")
        raise # Re-raise to mark the task as failed

@flow(name="CMDB Field Corrector Flow")
def fix_cmdb_flow(device_id: str, field_name: str, field_value: str):
    """
    Corrects a specific CMDB field for a given device by updating it via the Boomi integration endpoint.
    This flow is designed to remediate issues identified in other processes.

    Args:
        device_id: The ID of the device to update.
        field_name: The name of the field to update (e.g., "location", "status").
        field_value: The new value for the specified field.
    
    Raises:
        Exception: If the CMDB update task fails or returns a non-success status.
    """
    logger = get_run_logger()
    logger.info(
        f"Starting CMDB correction for device '{device_id}', "
        f"field '{field_name}' with value '{field_value}'."
    )

    try:
        # Submit the task to update CMDB
        result_future = update_cmdb_via_boomi.submit(
            device_id=device_id,
            field_name=field_name,
            field_value=field_value
        )
        result = result_future.result() # Wait for the task to complete and get its result

        if result and result.get("status") == "SUCCESS":
            logger.info(f"CMDB update flow completed successfully for device '{device_id}'.")
            # Return a clear success message for the flow run state
            return {"status": "SUCCESS", "message": f"Device '{device_id}' field '{field_name}' updated to '{field_value}'."}
        else:
            # This path is typically taken if the task returned a non-success status
            # rather than raising an exception, but our task always raises on failure.
            # Included for defensive programming.
            error_message = f"CMDB update task did not report SUCCESS for device '{device_id}'. Details: {result}"
            logger.error(error_message)
            raise Exception(error_message)

    except Exception as e:
        logger.error(f"CMDB correction flow failed due to an exception: {e}")
        # Re-raise the exception to mark the flow as failed with the specific error
        raise

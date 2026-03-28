import os
import json
import requests
from prefect import flow, task, get_run_logger

# Retrieve Boomi API Key from environment variable.
# This check is performed at module load time to ensure the key is available before execution.
BOOMI_API_KEY = os.getenv("BOOMI_API_KEY")
if not BOOMI_API_KEY:
    raise ValueError("BOOMI_API_KEY environment variable not set. Please set it to proceed.")

@task(retries=3, retry_delay_seconds=10)
def update_cmdb_via_boomi(device_id: str, field_name: str, field_value: str):
    """
    Task to update a specific CMDB device field via the Boomi integration endpoint.
    
    This task adheres to the following rules:
    - Retries up to 3 times with a 10-second delay on timeout or other transient network issues.
    - Logs the outgoing payload (excluding secrets).
    - Raises an exception if the HTTP response code is not 200.
    - Parses and logs the JSON response if successful.
    
    Args:
        device_id: The ID of the device to update.
        field_name: The name of the field to update.
        field_value: The new value for the specified field.
    
    Returns:
        dict: The JSON response from the Boomi API if successful.
    
    Raises:
        Exception: If the Boomi API call fails (non-200 status, network error, or invalid JSON response).
        requests.exceptions.Timeout: Re-raised to trigger Prefect's task retry mechanism on timeouts.
    """
    logger = get_run_logger()
    boomi_endpoint = "https://boomi.example.internal/api/cmdb/update"

    headers = {
        "Content-Type": "application/json",
        "X-API-Key": BOOMI_API_KEY, # X-API-Key is a secret and is in headers, not in the logged payload
    }

    payload = {
        "device_id": device_id,
        "field_name": field_name,
        "field_value": field_value,
        "source": "prefect_ai_remediation",
    }

    # Log the outgoing payload (excluding secrets)
    logger.info(f"Preparing to send CMDB update to Boomi for device '{device_id}', field '{field_name}'. "
                f"Payload: {json.dumps(payload)}")

    try:
        # Add a general timeout for the request to prevent indefinite hanging
        response = requests.post(boomi_endpoint, headers=headers, json=payload, timeout=30)

        if response.status_code != 200:
            logger.error(f"Boomi API update failed for device '{device_id}', field '{field_name}'. "
                         f"Status Code: {response.status_code}, Response: {response.text}")
            raise Exception(f"Boomi API update failed with status code {response.status_code} "
                            f"for device '{device_id}', field '{field_name}'.")

        try:
            remediation_result = response.json()
            logger.info(f"CMDB update successful for device '{device_id}', field '{field_name}'. "
                        f"Boomi response: {json.dumps(remediation_result)}")
            return remediation_result
        except json.JSONDecodeError:
            logger.error(f"Failed to parse JSON response from Boomi for device '{device_id}', field '{field_name}'. "
                         f"Response text: {response.text}", exc_info=True)
            raise Exception(f"Invalid JSON response from Boomi for device '{device_id}'.")

    except requests.exceptions.Timeout as e:
        # Log a warning and re-raise to trigger Prefect's task retry for timeouts
        logger.warning(f"Timeout while connecting to Boomi API for device '{device_id}', field '{field_name}'. Retrying...", exc_info=True)
        raise e
    except requests.exceptions.RequestException as e:
        # Catch other requests-related errors (e.g., connection errors, DNS failures)
        logger.error(f"An error occurred while calling Boomi API for device '{device_id}', field '{field_name}': {e}", exc_info=True)
        raise Exception(f"Network or request error during CMDB update for device '{device_id}': {e}")


@flow(name="CMDB Field Corrector Flow")
def fix_cmdb_flow(
    device_id: str,
    field_name: str,
    field_value: str
):
    """
    Corrective Prefect flow to update a specific field for a given device in CMDB
    via the Boomi integration endpoint.

    This flow acts as a remediation for device field inconsistencies or errors detected
    in other processes (e.g., 'device_loader.py').

    Args:
        device_id: The ID of the device to update (e.g., 'router-02').
        field_name: The name of the CMDB field to update (e.g., 'source_status', 'location').
        field_value: The new value for the specified field.
    
    Returns:
        dict: A dictionary indicating the status and result of the correction.
    
    Raises:
        Exception: If the CMDB update process fails for any reason.
    """
    logger = get_run_logger()
    logger.info(f"Starting CMDB field correction flow for device '{device_id}': "
                f"setting '{field_name}' to '{field_value}'.")

    try:
        remediation_result = update_cmdb_via_boomi(
            device_id=device_id,
            field_name=field_name,
            field_value=field_value
        )
        logger.info(f"CMDB correction flow completed successfully for device '{device_id}'.")
        return {"status": "success", "result": remediation_result}
    except Exception as e:
        logger.error(f"CMDB correction flow failed for device '{device_id}', field '{field_name}': {e}")
        raise # Re-raise the exception to mark the flow as failed

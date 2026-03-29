import os
import requests
import json
from prefect import flow, task, get_run_logger
from typing import List

# Business documentation constants
BOOMI_ENDPOINT = "https://boomi.example.internal/api/cmdb/update"
SOURCE_SYSTEM = "prefect_ai_remediation" # Fixed as per business documentation

@task(retries=3, retry_delay_seconds=10)
def update_cmdb_field(device_id: str, field_name: str, field_value: str):
    """
    Updates a specific field for a device in CMDB via the Boomi integration endpoint.
    Includes logging, retries, and error handling as per business rules.
    """
    logger = get_run_logger()

    # Retrieve API key from environment variable
    api_key = os.getenv("BOOMI_API_KEY")
    if not api_key:
        logger.error("BOOMI_API_KEY environment variable not set.")
        raise ValueError("BOOMI_API_KEY environment variable not set. Please set it as an environment variable.")

    headers = {
        "Content-Type": "application/json",
        "X-API-Key": api_key
    }

    payload = {
        "device_id": device_id,
        "field_name": field_name,
        "field_value": field_value,
        "source": SOURCE_SYSTEM # Fixed as per business rules
    }

    # Log outgoing payload excluding secrets
    log_payload = {k: v for k, v in payload.items()}
    logger.info(f"Attempting to update CMDB for device '{device_id}'. "
                f"Outgoing payload (excluding API key): {json.dumps(log_payload)}")

    try:
        # Add a reasonable timeout for the HTTP request itself
        response = requests.post(BOOMI_ENDPOINT, headers=headers, json=payload, timeout=60)

        # Raise HTTPError for bad responses (4xx or 5xx)
        # This will cause the task to fail, triggering Prefect's retry mechanism if configured.
        response.raise_for_status()

        remediation_result = response.json()
        logger.info(f"Successfully updated CMDB for device '{device_id}'. Result: {json.dumps(remediation_result)}")
        return remediation_result

    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout updating CMDB for device '{device_id}' after multiple attempts: {e}")
        # Re-raise to trigger Prefect task retry
        raise

    except requests.exceptions.RequestException as e:
        # This catches all other requests-related exceptions (connection errors, HTTP errors etc.)
        status_code_info = f"HTTP Status: {response.status_code}" if 'response' in locals() else "No HTTP response received"
        response_content_info = f"Response: {response.text}" if 'response' in locals() and response.content else "No response content"
        logger.error(f"Failed to update CMDB for device '{device_id}'. {status_code_info}. {response_content_info}. Error: {e}")
        raise # Re-raise to mark task as failed

    except json.JSONDecodeError as e:
        # Catches error if response is not valid JSON, but status code was 200
        response_text = response.text if 'response' in locals() else "N/A"
        logger.error(f"Failed to parse JSON response from Boomi API for device '{device_id}'. Error: {e}. Response text: {response_text}")
        raise

@flow(name="CMDB Remediation Flow",
      description="Corrective flow to update CMDB records for devices identified as 'source record not found'.")
def fix_cmdb_flow(
    device_ids: List[str] = ["router-23", "fw-02"], # Default devices from the analysis logs
    field_name: str = "status", # Plausible field to update for "source record not found" remediation
    field_value: str = "active" # Plausible value for the chosen field
):
    """
    Main flow to remediate CMDB issues for a list of devices.
    It orchestrates calls to the Boomi integration endpoint to update a specified field
    for each device.
    """
    logger = get_run_logger()
    logger.info(f"Starting CMDB remediation flow for devices: {device_ids}")
    logger.info(f"Attempting to set field '{field_name}' to value '{field_value}'. "
                f"Source system for update: '{SOURCE_SYSTEM}'.")

    futures = []
    for device_id in device_ids:
        # Submit the task for each device, Prefect handles concurrency
        future = update_cmdb_field.submit(
            device_id=device_id,
            field_name=field_name,
            field_value=field_value
        )
        futures.append(future)

    # Collect results and report on successes/failures
    successful_devices = []
    failed_devices_with_errors = {}

    for i, future in enumerate(futures):
        device_id = device_ids[i]
        try:
            # .result() will await the task's completion and raise its exception if it failed
            future.result()
            successful_devices.append(device_id)
        except Exception as e:
            failed_devices_with_errors[device_id] = str(e)

    if failed_devices_with_errors:
        logger.error(f"CMDB remediation completed with partial failures. "
                     f"Successfully updated devices: {successful_devices if successful_devices else 'None'}. "
                     f"Failed devices and their errors: {json.dumps(failed_devices_with_errors)}")
        # For a corrective flow, if some remediations fail, the flow itself should indicate a failure
        raise Exception(f"Failed to remediate CMDB for some devices: {list(failed_devices_with_errors.keys())}")
    else:
        logger.info(f"All specified devices ({len(successful_devices)}) successfully remediated in CMDB.")

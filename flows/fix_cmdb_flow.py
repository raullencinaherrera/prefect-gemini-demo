import os
import json
import requests
from prefect import flow, task, get_run_logger
from prefect.exceptions import MissingResult

# Business documentation constants
BOOMI_ENDPOINT = "https://boomi.example.internal/api/cmdb/update"
SOURCE_NAME = "prefect_ai_remediation"

# Prefect task for updating a single CMDB entry via Boomi
@task(retries=3, retry_delay_seconds=10) # Retry up to 3 times on any exception, including timeouts
def update_cmdb_entry(device_id: str, field_name: str, field_value: str) -> dict:
    """
    Updates a single CMDB entry via the Boomi integration endpoint.

    Args:
        device_id: The ID of the device to update.
        field_name: The name of the field to update.
        field_value: The new value for the field.

    Returns:
        A dictionary containing the Boomi API response and remediation status.

    Raises:
        Exception: If the BOOMI_API_KEY environment variable is not set,
                   or if the Boomi API call fails (non-200 status code,
                   or other request exceptions after all retries).
    """
    logger = get_run_logger()

    boomi_api_key = os.getenv("BOOMI_API_KEY")
    if not boomi_api_key:
        raise Exception("BOOMI_API_KEY environment variable is not set.")

    headers = {
        "Content-Type": "application/json",
        "X-API-Key": boomi_api_key,
    }

    payload = {
        "device_id": device_id,
        "field_name": field_name,
        "field_value": field_value,
        "source": SOURCE_NAME,
    }

    # Log the outgoing payload, excluding secrets (API key is in headers)
    logger.info(f"Sending CMDB update request for device '{device_id}': {json.dumps(payload)}")

    try:
        # Using a timeout for the request itself to prevent indefinite waits.
        # Prefect retries will handle network issues or Boomi timeouts.
        response = requests.post(BOOMI_ENDPOINT, headers=headers, json=payload, timeout=30)
        response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)

        remediation_result = response.json()
        logger.info(f"CMDB update for device '{device_id}' successful. Boomi response: {json.dumps(remediation_result)}")
        return {"status": "SUCCESS", "device_id": device_id, "result": remediation_result}

    except requests.exceptions.HTTPError as e:
        error_message = f"Boomi API call failed with status {e.response.status_code} for device '{device_id}'. Response: {e.response.text}"
        logger.error(error_message)
        raise Exception(error_message) from e
    except requests.exceptions.ConnectionError as e:
        error_message = f"Failed to connect to Boomi API for device '{device_id}'. Connection error: {e}"
        logger.error(error_message)
        raise Exception(error_message) from e
    except requests.exceptions.Timeout as e:
        error_message = f"Boomi API call timed out for device '{device_id}'. Error: {e}"
        logger.error(error_message)
        raise Exception(error_message) from e
    except requests.exceptions.RequestException as e:
        error_message = f"An unexpected request error occurred for device '{device_id}': {e}"
        logger.error(error_message)
        raise Exception(error_message) from e
    except json.JSONDecodeError as e:
        error_message = f"Failed to parse JSON response from Boomi API for device '{device_id}'. Error: {e}. Raw response: {response.text}"
        logger.error(error_message)
        raise Exception(error_message) from e

@flow(name="CMDB Remediation Flow")
def fix_cmdb_flow(cmdb_updates: list[dict]):
    """
    A corrective Prefect flow to update CMDB entries via the Boomi integration.

    This flow takes a list of CMDB update requests, processes each one as a task,
    and logs the outcome. It's designed to remediate specific CMDB data issues.

    Args:
        cmdb_updates: A list of dictionaries, where each dictionary represents
                      a single CMDB update operation. Each dictionary must
                      contain 'device_id', 'field_name', and 'field_value' keys.
                      Example:
                      [
                          {"device_id": "router-23", "field_name": "status", "field_value": "active"},
                          {"device_id": "fw-02", "field_name": "location", "field_value": "datacenter-a"}
                      ]
    """
    logger = get_run_logger()
    logger.info(f"Starting CMDB remediation flow for {len(cmdb_updates)} potential updates.")

    if not cmdb_updates:
        logger.info("No CMDB updates provided. Flow finished.")
        return

    update_futures = []
    for update in cmdb_updates:
        required_keys = ["device_id", "field_name", "field_value"]
        if not all(key in update for key in required_keys):
            logger.warning(
                f"Skipping malformed update request: {update}. "
                f"Missing one of the required keys: {', '.join(required_keys)}."
            )
            continue
        
        # Submit the task for each update
        future = update_cmdb_entry.submit(
            device_id=update["device_id"],
            field_name=update["field_name"],
            field_value=update["field_value"]
        )
        update_futures.append(future)

    successful_updates = []
    failed_updates = []

    for future in update_futures:
        update_info = future.kwargs # Extract original task arguments for logging context
        device_id = update_info.get("device_id", "UNKNOWN")

        try:
            # Attempt to get the task's result. This will raise an exception if the task failed
            result = future.result()
            successful_updates.append(result)
        except MissingResult:
            # This can happen if the task itself was cancelled or never ran to completion
            failed_updates.append({"device_id": device_id, "status": "FAILED", "reason": "Task did not produce a result (e.g., cancelled or internal Prefect error)"})
            logger.error(f"CMDB update task for device '{device_id}' failed: Task did not produce a result.")
        except Exception as e:
            # Catch the exception from the failed task after all retries
            failed_updates.append({"device_id": device_id, "status": "FAILED", "reason": str(e)})
            logger.error(f"CMDB update task for device '{device_id}' ultimately failed after retries: {e}")

    logger.info(f"CMDB remediation flow finished.")
    logger.info(f"Total CMDB updates attempted: {len(update_futures)}")
    logger.info(f"Successful updates: {len(successful_updates)}")
    for s_update in successful_updates:
        logger.debug(f"  - Device '{s_update['device_id']}' remediated successfully.")
    
    if failed_updates:
        logger.error(f"Failed updates: {len(failed_updates)}")
        for f_update in failed_updates:
            logger.error(f"  - Device '{f_update['device_id']}' failed: {f_update['reason']}")
        # Depending on business requirements, you might want to raise an exception here
        # to mark the entire flow as failed if any individual remediation task fails.
        # For now, the flow will complete but log errors.
        # raise Exception(f"Some CMDB updates failed: {failed_updates}")
    else:
        logger.info("All submitted CMDB updates completed successfully.")
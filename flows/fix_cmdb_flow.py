import os
import httpx
import json
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from typing import List, Dict, Any


class BoomiAPIError(Exception):
    """Custom exception for errors originating from the Boomi API integration."""
    pass


@task(
    retries=3,  # Retry up to 3 times
    retry_delay_seconds=10,  # Wait 10 seconds between retries
    name="Update CMDB Field via Boomi"
)
async def update_cmdb_field_via_boomi(
    device_id: str,
    field_name: str,
    field_value: str
) -> Dict[str, Any]:
    logger = get_run_logger()

    # Retrieve Boomi API key from Prefect Secret block for security best practices
    # The Secret block named 'BOOMI_API_KEY' must be configured in your Prefect workspace.
    try:
        boomi_api_key_secret = await Secret.load("BOOMI_API_KEY")
        boomi_api_key = boomi_api_key_secret.get()
    except Exception as e:
        logger.error(f"Failed to load Boomi API Key from Prefect Secret block: {e}")
        raise RuntimeError("BOOMI_API_KEY Prefect Secret block not found or inaccessible.") from e

    url = "https://boomi.example.internal/api/cmdb/update"
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": boomi_api_key, # Secret handled by Prefect Secret block
    }
    payload = {
        "device_id": device_id,
        "field_name": field_name,
        "field_value": field_value,
        "source": "prefect_ai_remediation" # Source of this specific update request
    }

    # Log the outgoing payload, excluding the API key (which is in headers, not payload)
    # For robust logging, ensure only non-sensitive parts are logged.
    log_payload_str = json.dumps(payload, indent=2)
    logger.info(f"Attempting to update CMDB for device '{device_id}' via Boomi. Outgoing payload:\n{log_payload_str}")

    try:
        # Using httpx for asynchronous HTTP requests with a client-level timeout
        async with httpx.AsyncClient(timeout=30.0) as client: # 30 second timeout for the request
            response = await client.post(url, headers=headers, json=payload)

        # Raise an exception for HTTP status codes 4xx or 5xx
        response.raise_for_status()

        # Parse JSON response as per documentation
        remediation_result = response.json()
        logger.info(f"Successfully updated CMDB for device '{device_id}'. Boomi response: {remediation_result}")
        return remediation_result

    except httpx.TimeoutException:
        # This handles cases where the request itself times out. Prefect retries will activate.
        logger.error(f"Boomi API request timed out for device '{device_id}'.")
        raise # Re-raise to trigger Prefect task retries
    except httpx.HTTPStatusError as e:
        # Specific handling for non-2xx responses
        error_msg = f"Boomi API returned an error for device '{device_id}': {e.response.status_code} - {e.response.text}"
        logger.error(error_msg)
        raise BoomiAPIError(error_msg) from e
    except json.JSONDecodeError:
        # Handling for non-JSON responses when JSON is expected
        error_msg = f"Boomi API response for device '{device_id}' was not valid JSON. Raw response: {response.text}"
        logger.error(error_msg)
        raise BoomiAPIError(error_msg)
    except Exception as e:
        # Catch any other unexpected errors during the HTTP call
        error_msg = f"An unexpected error occurred during Boomi API call for device '{device_id}': {type(e).__name__}: {e}"
        logger.error(error_msg)
        raise BoomiAPIError(error_msg) from e


@flow(name="Fix CMDB Flow: Device Source Remediation")
async def fix_cmdb_device_sources(
    device_ids: List[str],
    field_to_update: str = "source", # Default field to update based on 'source record not found' error
    field_value_to_set: str = "manual_remediation" # Default value for the 'source' field in CMDB
):
    """
    A corrective Prefect flow to update a specified CMDB field for a list of devices
    via the Boomi integration endpoint. This flow is designed to remediate
    'source record not found' errors by setting a default source value.

    Args:
        device_ids: A list of device IDs whose CMDB records need updating.
        field_to_update: The name of the CMDB field to update (e.g., 'source').
        field_value_to_set: The value to set for the specified CMDB field.
    """
    logger = get_run_logger()
    logger.info(f"Starting CMDB remediation for {len(device_ids)} devices: {device_ids}")
    logger.info(f"Attempting to set CMDB field '{field_to_update}' to '{field_value_to_set}'.")

    update_results = []
    for device_id in device_ids:
        try:
            # Call the task to update each device's CMDB record
            result = await update_cmdb_field_via_boomi( # Await to get the result from the task
                device_id=device_id,
                field_name=field_to_update,
                field_value=field_value_to_set
            )
            update_results.append({"device_id": device_id, "status": "SUCCESS", "result": result})
        except Exception as e:
            logger.error(f"Failed to remediate CMDB for device '{device_id}': {e}")
            update_results.append({"device_id": device_id, "status": "FAILED", "error": str(e)})

    logger.info("CMDB remediation process complete.")

    # Summarize results and raise if any updates failed
    failed_updates = [r for r in update_results if r["status"] == "FAILED"]
    if failed_updates:
        error_message = f"CMDB remediation failed for {len(failed_updates)} out of {len(device_ids)} devices."
        logger.error(f"{error_message} Failed devices: {json.dumps(failed_updates, indent=2)}")
        raise Exception(error_message)
    else:
        logger.info("All specified devices successfully updated in CMDB.")

    return update_results


if __name__ == "__main__":
    # Example usage based on the analysis of the original flow's failures
    failed_devices = [
        "router-02",
        "router-23",
        "switch-05",
    ]

    # The flow will update the 'source' field to 'manual_remediation' for these devices
    fix_cmdb_device_sources(device_ids=failed_devices)

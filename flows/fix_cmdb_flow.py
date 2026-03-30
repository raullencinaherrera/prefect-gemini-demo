import os
import re
import json
import httpx
from prefect import flow, task, get_run_logger
from typing import List, Dict, Any, Optional

# Task to extract device_id and determine update fields from a failure message
@task
def parse_failure_message_for_cmdb_update(message: str) -> Optional[Dict[str, str]]:
    logger = get_run_logger()
    
    # Pattern to extract device_id from "CMDB validation error: source record not found for device <device_id>"
    match_cmdb_not_found = re.search(r"CMDB validation error: source record not found for device (\S+)", message)
    
    if match_cmdb_not_found:
        device_id = match_cmdb_not_found.group(1)
        logger.info(f"Identified device_id '{device_id}' from message: '{message}'")
        # As per analysis, if "source record not found", we assume a general status update.
        # The business logic doesn't specify a field, so 'status':'provisioned' is a reasonable default action.
        return {
            "device_id": device_id,
            "field_name": "status", 
            "field_value": "provisioned" 
        }
    
    # For now, only handle "source record not found" explicitly for CMDB updates.
    # Other failure types (e.g., 'Timeout connecting to API') are not directly actionable
    # via a CMDB field update without further context.
    
    logger.warning(f"Could not determine CMDB update fields from message: '{message}'. Skipping remediation for this message.")
    return None

# Task to send the CMDB update via Boomi
@task(retries=3, retry_delay_seconds=5) # Retry up to 3 times on timeout as per business rule
async def send_cmdb_update_via_boomi(update_payload: Dict[str, Any]) -> Dict[str, Any]:
    logger = get_run_logger()
    
    BOOMI_ENDPOINT = "https://boomi.example.internal/api/cmdb/update"
    
    # Retrieve BOOMI_API_KEY from environment variable as per business rule
    boomi_api_key = os.getenv("BOOMI_API_KEY")
    if not boomi_api_key:
        logger.error("BOOMI_API_KEY environment variable not set. Cannot send CMDB update.")
        raise ValueError("BOOMI_API_KEY environment variable not set.")

    headers = {
        "Content-Type": "application/json",
        "X-API-Key": boomi_api_key # Use environment variable BOOMI_API_KEY
    }

    # Log outgoing payload excluding secrets as per business rule
    # The X-API-Key is in headers, not the JSON payload, so logging payload is safe.
    loggable_payload = update_payload.copy()
    logger.info(f"Sending CMDB update request to Boomi. Payload: {json.dumps(loggable_payload)}")

    try:
        # Use an AsyncClient for non-blocking I/O with a sensible timeout
        async with httpx.AsyncClient(timeout=10.0) as client: 
            response = await client.post(BOOMI_ENDPOINT, headers=headers, json=update_payload)
            
            # Raise an exception if response code is not 200 as per business rule
            response.raise_for_status() 
            
            # Parse JSON response and log remediation result as per business rule
            remediation_result = response.json()
            logger.info(f"CMDB update successful for device '{update_payload.get('device_id')}'. Result: {json.dumps(remediation_result)}")
            return remediation_result
            
    except httpx.TimeoutException:
        logger.error(f"Boomi API request timed out for device '{update_payload.get('device_id')}'. Retrying...")
        raise # Re-raise to trigger Prefect task retry mechanism
    except httpx.HTTPStatusError as e:
        logger.error(f"Boomi API returned non-200 status code {e.response.status_code} for device '{update_payload.get('device_id')}'. Response: {e.response.text}")
        raise # Re-raise the exception
    except json.JSONDecodeError:
        logger.error(f"Boomi API response was not valid JSON for device '{update_payload.get('device_id')}'. Response: {response.text}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during Boomi API call for device '{update_payload.get('device_id')}': {e}")
        raise

@flow(name="CMDB Remediation Flow")
async def fix_cmdb_flow(failed_device_messages: List[str]):
    """
    Corrective Prefect flow to remediate CMDB update failures by sending
    updates to the Boomi integration endpoint.

    Args:
        failed_device_messages: A list of error messages indicating device load failures,
                                typically extracted from a prior flow's exception message.
                                E.g., ['CMDB validation error: source record not found for device router-02', ...]
    """
    logger = get_run_logger()
    logger.info(f"Starting CMDB remediation flow for {len(failed_device_messages)} reported issues.")
    
    remediation_results = []
    
    for message in failed_device_messages:
        # Step 1: Parse the failure message to identify device and required update fields
        parsed_info = await parse_failure_message_for_cmdb_update(message)
        
        if parsed_info:
            device_id = parsed_info["device_id"]
            field_name = parsed_info["field_name"]
            field_value = parsed_info["field_value"]
            
            payload = {
                "device_id": device_id,
                "field_name": field_name,
                "field_value": field_value,
                "source": "prefect_ai_remediation" # As per business documentation
            }
            
            # Step 2: Send the CMDB update via Boomi
            try:
                result = await send_cmdb_update_via_boomi(payload)
                remediation_results.append({
                    "device_id": device_id,
                    "status": "SUCCESS",
                    "result": result
                })
            except Exception as e:
                logger.error(f"Failed to remediate device '{device_id}'. Error: {e}")
                remediation_results.append({
                    "device_id": device_id,
                    "status": "FAILED",
                    "error": str(e)
                })
        else:
            logger.info(f"Skipping remediation for message '{message}' as no CMDB update could be determined.")
            remediation_results.append({
                "message": message,
                "status": "SKIPPED",
                "reason": "No parsable CMDB update information"
            })
            
    logger.info(f"CMDB remediation flow finished. Total results: {len(remediation_results)}")
    return remediation_results

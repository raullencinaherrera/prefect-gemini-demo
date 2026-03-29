import asyncio
from datetime import datetime, timedelta
import logging

from prefect import flow, task

# Configure basic logging for better visibility within the script itself.
# Prefect's logging will take over when run as a flow, but this helps for local script testing.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Mock API Data Store ---
# In a real scenario, this would be a database or another service.
# For simulation, we'll keep a simple in-memory dictionary acting as our 'database'.
_MOCK_DEVICE_DB = {
    "device-xyz-123": {
        "id": "device-xyz-123",
        "name": "Smart Thermostat Pro",
        "status": "online",
        "firmware_version": "2.1.0",
        "location": "Living Room",
        "temperature_celsius": 22.5,
        "battery_level": 85,
        "last_seen": datetime.utcnow() - timedelta(minutes=5)
    }
}

@task
async def fetch_device_data(device_id: str) -> dict:
    """
    Simulates fetching current device data from an external API.

    Args:
        device_id: The ID of the device to fetch.

    Returns:
        A dictionary containing the device's current data.

    Raises:
        ValueError: If the device is not found in the mock database.
    """
    logger.info(f"Simulating API call: GET /devices/{device_id}")
    await asyncio.sleep(1)  # Simulate network latency

    device_data = _MOCK_DEVICE_DB.get(device_id)
    if not device_data:
        raise ValueError(f"Device with ID '{device_id}' not found in mock DB.")

    logger.info(f"Fetched device data for {device_id}: {device_data}")
    return device_data.copy() # Return a copy to prevent accidental modification outside tasks

@task
def generate_update_payload(current_data: dict, new_status: str, new_firmware: str) -> dict:
    """
    Generates a payload with new field values for a device update.

    Args:
        current_data: The current data of the device, used to derive some new values.
        new_status: The new status to set for the device.
        new_firmware: The new firmware version to set.

    Returns:
        A dictionary representing the update payload (partial fields to change).
    """
    logger.info("Generating update payload...")
    updated_fields = {
        "status": new_status,
        "firmware_version": new_firmware,
        "battery_level": current_data.get("battery_level", 100) - 10, # Simulate battery drain
        "temperature_celsius": current_data.get("temperature_celsius", 20) + 0.5, # Small change
        "last_updated_by_flow": datetime.utcnow().isoformat() # Mark that the flow updated it
    }
    logger.info(f"Generated payload: {updated_fields}")
    return updated_fields

@task
async def simulate_api_update(device_id: str, update_payload: dict) -> dict:
    """
    Simulates sending an update request (e.g., PATCH) to an external device API.
    Updates the mock database and returns a simulated API response.

    Args:
        device_id: The ID of the device to update.
        update_payload: A dictionary of fields to update.

    Returns:
        A dictionary representing the device's state after the update,
        as typically returned by a successful API call, including a 'success' status.

    Raises:
        ValueError: If the device is not found in the mock database during the update.
    """
    logger.info(f"Simulating API call: PATCH /devices/{device_id} with payload: {update_payload}")
    await asyncio.sleep(2)  # Simulate API processing time

    # Simulate database update
    current_device_in_db = _MOCK_DEVICE_DB.get(device_id)
    if not current_device_in_db:
        raise ValueError(f"Device with ID '{device_id}' not found in mock DB for update.")

    current_device_in_db.update(update_payload)
    current_device_in_db["last_seen"] = datetime.utcnow() # Simulate 'last seen' being updated by device
    
    # Simulate API response which typically returns the full updated resource
    simulated_api_response = {
        "success": True,
        "message": f"Device {device_id} updated successfully.",
        "updated_device": current_device_in_db.copy() # Return a copy of the updated state
    }
    
    logger.info(f"API update simulated for {device_id}. Response: {simulated_api_response['message']}")
    return simulated_api_response

@task
async def verify_device_status_after_update(device_id: str) -> dict:
    """
    Simulates re-fetching the device status after an update to verify changes.
    This uses the same mock DB, reflecting a consistent state after the simulated update.

    Args:
        device_id: The ID of the device to verify.

    Returns:
        A dictionary containing the device's data after verification.

    Raises:
        ValueError: If the device is not found during verification.
    """
    logger.info(f"Simulating API call: GET /devices/{device_id} to verify update.")
    await asyncio.sleep(1) # Simulate network latency
    
    verified_data = _MOCK_DEVICE_DB.get(device_id)
    if not verified_data:
        raise ValueError(f"Device with ID '{device_id}' disappeared after update!")
        
    logger.info(f"Verified device data for {device_id}: {verified_data}")
    return verified_data.copy()

@flow(name="Device API Simulator Flow")
async def device_api_simulator_flow(device_id: str = "device-xyz-123", target_status: str = "maintenance", target_firmware: str = "2.1.1") -> None:
    """
    A Prefect flow to simulate connecting to an external API to fetch device data
    and then updating some of its fields.

    The flow consists of the following steps:
    1. Fetch the current state of a device.
    2. Generate a payload with desired new field values.
    3. Simulate sending this update payload to a device API.
    4. (Optional) Re-fetch the device's state to independently verify the changes.

    Args:
        device_id: The ID of the device to simulate operations on.
        target_status: The new status to set for the device (e.g., "online", "offline", "maintenance").
        target_firmware: The new firmware version to set for the device (e.g., "2.1.1").
    """
    logger.info(f"Starting device API simulation for device: {device_id}")

    try:
        # 1. Fetch initial device data
        initial_device_data = await fetch_device_data(device_id)

        # 2. Prepare the update payload based on initial data and target values
        update_payload = generate_update_payload(initial_device_data, target_status, target_firmware)

        # 3. Simulate sending the update request to the API
        api_response = await simulate_api_update(device_id, update_payload)
        
        # Check if the update was successful from the simulated API response
        if api_response.get("success"):
            logger.info(f"Update reported as successful by simulated API. Updated device details: {api_response['updated_device']}")
            
            # 4. (Optional but good practice) Re-fetch the device data to verify the update independently
            verified_device_data = await verify_device_status_after_update(device_id)
            
            logger.info("\n--- Flow Summary ---")
            logger.info(f"Initial device state: {initial_device_data}")
            logger.info(f"Update payload sent: {update_payload}")
            logger.info(f"Final verified device state: {verified_device_data}")
            logger.info("Flow completed successfully: Device fields were changed as requested.")
        else:
            logger.error(f"Simulated API reported an error during update: {api_response.get('message', 'Unknown error')}")
            logger.error("Flow completed with errors: Device fields might not have been changed.")

    except Exception as e:
        logger.error(f"An unexpected error occurred during the flow: {e}")
        raise # Re-raise to let Prefect mark the flow as failed


if __name__ == "__main__":
    # Example of how to run the flow locally using asyncio.run
    # This ensures async functions are properly executed.

    # Run with default parameters
    asyncio.run(device_api_simulator_flow())

    # Run with custom parameters
    # print("\n" + "="*50 + "\n")
    # asyncio.run(device_api_simulator_flow(device_id="device-xyz-123", target_status="operational", target_firmware="2.1.2"))

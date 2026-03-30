import time
import random
from typing import List, Dict, Any

from prefect import flow, task
from prefect.logging import get_run_logger

# --- Tasks ---

@task(retries=3, retry_delay_seconds=5)
def authenticate_servinow(api_key: str) -> str:
    """
    Simulates authenticating with the Servinow API to get an access token.
    Raises an error if authentication fails.
    """
    logger = get_run_logger()
    logger.info("Attempting to authenticate with Servinow API...")
    time.sleep(random.uniform(0.5, 1.5)) # Simulate network delay
    
    if not api_key or api_key == "your_servinow_secret_api_key":
        logger.error("Invalid or placeholder API Key provided. Authentication failed.")
        raise ValueError("Valid API Key is required for Servinow authentication.")
    
    # Simulate a successful authentication
    auth_token = f"simulated_servinow_token_{random.randint(1000, 9999)}"
    logger.info(f"Successfully authenticated. Token: {auth_token[:10]}...")
    return auth_token

@task
def check_device_readiness(device_id: str, auth_token: str) -> bool:
    """
    Simulates checking the readiness of a specific device via Servinow API.
    """
    logger = get_run_logger()
    logger.info(f"Checking readiness for device '{device_id}'...")
    time.sleep(random.uniform(0.2, 1.0)) # Simulate API call delay
    
    # Simulate random readiness status for demonstration (e.g., 80% ready)
    is_ready = random.random() < 0.8
    
    if is_ready:
        logger.info(f"Device '{device_id}' is ready for loading.")
    else:
        logger.error(f"Device '{device_id}' is NOT ready. Skipping load.")
    
    return is_ready

@task(retries=2, retry_delay_seconds=3)
def initiate_device_load(
    device_id: str,
    load_duration_seconds: int,
    auth_token: str
) -> str:
    """
    Simulates sending a command to a device via Servinow API to start loading.
    Returns a unique load session ID.
    Raises a RuntimeError if the load command fails to initiate.
    """
    logger = get_run_logger()
    logger.info(f"Initiating load for device '{device_id}' for {load_duration_seconds} seconds...")
    time.sleep(random.uniform(0.5, 2.0)) # Simulate API call and device start-up delay
    
    # Simulate potential failure to initiate load (e.g., 10% chance)
    if random.random() < 0.1:
        logger.error(f"Failed to initiate load for device '{device_id}'. API error or device issue.")
        raise RuntimeError(f"Failed to initiate load for device '{device_id}'.")
        
    load_session_id = f"load_session_{device_id}_{int(time.time())}"
    logger.info(f"Load initiated successfully for device '{device_id}'. Session ID: {load_session_id}")
    return load_session_id

@task
def wait_for_load_completion(
    device_id: str,
    load_session_id: str,
    expected_duration: int,
    auth_token: str
) -> bool:
    """
    Simulates monitoring a device's load process for completion.
    In a real scenario, this might poll an API endpoint or wait for a webhook.
    """
    logger = get_run_logger()
    logger.info(
        f"Monitoring load completion for device '{device_id}' "
        f"(Session: {load_session_id}). Expected duration: {expected_duration}s..."
    )
    
    # Simulate the actual load process by sleeping
    time.sleep(expected_duration) 
    
    # Simulate checking final status (e.g., 5% chance of load failure)
    if random.random() < 0.05:
        logger.error(f"Load failed unexpectedly for device '{device_id}' (Session: {load_session_id}).")
        return False
        
    logger.info(f"Load process completed for device '{device_id}' (Session: {load_session_id}).")
    return True

@task
def finalize_device_load(
    device_id: str,
    load_session_id: str,
    auth_token: str
) -> Dict[str, Any]:
    """
    Simulates sending a finalization command or retrieving post-load data from Servinow API.
    """
    logger = get_run_logger()
    logger.info(f"Finalizing load for device '{device_id}' (Session: {load_session_id})...")
    time.sleep(random.uniform(0.3, 1.2)) # Simulate API call delay
    
    # Simulate some final result data
    result = {
        "device_id": device_id,
        "load_session_id": load_session_id,
        "status": "completed",
        "timestamp": time.time(),
        "data_loaded_mb": random.randint(100, 1000)
    }
    
    logger.info(f"Load finalized for device '{device_id}'. Results: {result['status']}")
    return result

# --- Flow ---

@flow(name="Servinow Device Load Flow", log_prints=True)
def servinow_load_devices_flow(
    device_ids: List[str] = ["device-001", "device-002", "device-003", "device-004", "device-005"],
    default_load_duration_seconds: int = 10,
    servinow_api_key: str = "your_servinow_secret_api_key"
) -> List[Dict[str, Any]]:
    """
    Orchestrates the process of loading multiple devices using a simulated Servinow API.

    Args:
        device_ids: A list of device identifiers to load.
        default_load_duration_seconds: The default duration in seconds for each device load.
        servinow_api_key: The API key for authenticating with the Servinow API.

    Returns:
        A list of dictionaries, each containing the finalization results for successfully loaded devices.
    """
    logger = get_run_logger()
    logger.info(f"Starting Servinow Device Load Flow for {len(device_ids)} devices.")

    # 1. Authenticate with Servinow API
    auth_token = None
    try:
        auth_token = authenticate_servinow(servinow_api_key)
    except ValueError as e:
        logger.critical(f"Flow cannot proceed due to authentication error: {e}")
        return []
    except Exception as e:
        logger.critical(f"An unexpected error occurred during authentication: {e}")
        return []

    final_results = []
    
    # 2. Process each device
    for device_id in device_ids:
        logger.info(f"\n--- Processing device: {device_id} ---")
        try:
            # 2a. Check device readiness
            is_ready = check_device_readiness(device_id, auth_token)
            if not is_ready:
                logger.warning(f"Device '{device_id}' is not ready. Skipping load.")
                continue # Move to the next device
            
            # 2b. Initiate device load
            load_session_id = initiate_device_load(device_id, default_load_duration_seconds, auth_token)
            
            # 2c. Wait for load completion
            load_successful = wait_for_load_completion(
                device_id, load_session_id, default_load_duration_seconds, auth_token
            )
            
            if not load_successful:
                logger.error(f"Load process for device '{device_id}' (Session: {load_session_id}) did not complete successfully. Skipping finalization.")
                continue # Move to the next device
                
            # 2d. Finalize device load
            result = finalize_device_load(device_id, load_session_id, auth_token)
            final_results.append(result)
            
        except Exception as e:
            logger.error(f"An error occurred while processing device '{device_id}': {e}", exc_info=True)
            # The flow is designed to continue with other devices even if one fails.
            
    logger.info(f"Servinow Device Load Flow completed. Successfully processed {len(final_results)} devices out of {len(device_ids)} targeted.")
    return final_results

# --- Main execution block (for local testing) ---
if __name__ == "__main__":
    # To run this flow:
    # 1. Save it as servinow_load_devices_flow.py
    # 2. Open your terminal in the same directory
    # 3. Execute: python servinow_load_devices_flow.py
    
    # You can also customize parameters:
    # servinow_load_devices_flow(device_ids=["dev-a", "dev-b"], default_load_duration_seconds=5, servinow_api_key="my_actual_api_key_123")
    
    # Example 1: Run with default parameters
    print("\n--- Running flow with default parameters ---")
    servinow_load_devices_flow(servinow_api_key="my_actual_servinow_key") 
    
    # Example 2: Run with custom devices and a shorter duration
    # print("\n--- Running flow with custom devices and shorter duration ---")
    # custom_device_ids = ["warehouse-device-X", "factory-device-Y", "test-device-Z"]
    # servinow_load_devices_flow(device_ids=custom_device_ids, default_load_duration_seconds=5, servinow_api_key="my_actual_servinow_key")

    # Example 3: Demonstrate authentication failure
    # print("\n--- Running flow to demonstrate authentication failure ---")
    # servinow_load_devices_flow(device_ids=["problem-device"], servinow_api_key="") # Intentionally pass an invalid key

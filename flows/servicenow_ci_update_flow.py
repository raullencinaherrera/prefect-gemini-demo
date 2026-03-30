import time
from typing import Dict, Any, Optional

from prefect import flow, task, get_run_logger
# The following imports are for deployment configuration and are optional for the core flow logic
# They are included here for "production-style structure" demonstration.
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule

# --- Configuration Constants ---
# In a real scenario, these would come from environment variables,
# Prefect Blocks (e.g., Secrets block), or a dedicated config management.
SERVICENOW_BASE_URL: str = "https://your-instance.service-now.com/api/now"
SERVICENOW_USERNAME: str = "your_servicenow_user"
SERVICENOW_PASSWORD: str = "your_servicenow_password"
SIMULATION_DELAY_SECONDS: float = 1.0 # Simulate network latency or processing time

@task(retries=3, retry_delay_seconds=5)
def simulate_servicenow_authentication(
    username: str, password: str, base_url: str
) -> Optional[str]:
    """
    Simulates authenticating to ServiceNow.
    In a real scenario, this would involve API calls (e.g., OAuth, basic auth).
    Returns a dummy token or None on failure.
    """
    logger = get_run_logger()
    logger.info(f"Attempting to authenticate to ServiceNow instance: {base_url}...")
    time.sleep(SIMULATION_DELAY_SECONDS) # Simulate network delay

    # Simulate success/failure based on dummy credentials
    if username == SERVICENOW_USERNAME and password == SERVICENOW_PASSWORD:
        dummy_auth_token = f"dummy_token_for_{username}_{int(time.time())}"
        logger.info("ServiceNow authentication successful.")
        return dummy_auth_token
    else:
        logger.error("ServiceNow authentication failed: Invalid credentials.")
        return None

@task(retries=2, retry_delay_seconds=3)
def get_ci_data(
    ci_id: str, auth_token: str, base_url: str
) -> Optional[Dict[str, Any]]:
    """
    Simulates fetching Configuration Item (CI) data from ServiceNow.
    In a real scenario, this would be an API GET request.
    This task is included for completeness but is not directly called in the main flow
    as the request was primarily about "changing a field".
    """
    logger = get_run_logger()
    logger.info(f"Fetching CI data for CI ID '{ci_id}' using token '{auth_token[:10]}...'...")
    time.sleep(SIMULATION_DELAY_SECONDS * 1.5) # Simulate slightly longer fetch time

    # Simulate CI data retrieval
    simulated_cis = {
        "server_001": {"sys_id": "sysid_server_001", "name": "Prod Web Server 01", "ip_address": "192.168.1.10", "status": "operational", "owner": "IT Operations"},
        "database_005": {"sys_id": "sysid_db_005", "name": "CRM DB Cluster", "ip_address": "10.0.0.50", "status": "operational", "owner": "DB Team"},
        "router_abc": {"sys_id": "sysid_router_abc", "name": "Core Router A", "ip_address": "172.16.0.1", "status": "maintenance", "owner": "Network Team"}
    }

    if ci_id in simulated_cis:
        logger.info(f"Successfully retrieved data for CI ID '{ci_id}'.")
        return simulated_cis[ci_id]
    else:
        logger.warning(f"CI ID '{ci_id}' not found in ServiceNow (simulated).")
        return None

@task(retries=3, retry_delay_seconds=5)
def update_ci_field(
    ci_id: str,
    field_name: str,
    new_value: Any,
    auth_token: str,
    base_url: str,
) -> bool:
    """
    Simulates updating a field for a Configuration Item (CI) in ServiceNow.
    In a real scenario, this would be an API PATCH/PUT request.
    """
    logger = get_run_logger()
    logger.info(f"Attempting to update CI ID '{ci_id}': field '{field_name}' to '{new_value}'...")
    time.sleep(SIMULATION_DELAY_SECONDS * 2) # Simulate longer update time

    # Simulate the update process
    # In a real scenario, you'd send an HTTP request to ServiceNow
    # and parse its response to determine success.
    # Simple error simulation: if new_value contains "error" (case-insensitive)
    # or if the CI_ID is specifically "non_existent_ci" (to simulate CI not found at update step)
    if "error" in str(new_value).lower() or ci_id == "non_existent_ci":
        logger.error(f"Simulated error: Failed to update CI '{ci_id}'. New value '{new_value}' or CI ID '{ci_id}' triggered an error.")
        return False
    
    logger.info(f"Successfully simulated update for CI '{ci_id}': '{field_name}' set to '{new_value}'.")
    return True

@flow(name="ServiceNow CI Field Update Flow")
def servicenow_ci_update_flow(
    ci_id: str,
    field_to_update: str,
    new_field_value: str,
    username: str = SERVICENOW_USERNAME,
    password: str = SERVICENOW_PASSWORD,
    servicenow_url: str = SERVICENOW_BASE_URL,
) -> bool:
    """
    A Prefect flow to simulate connecting to ServiceNow and updating a specific
    field for a given Configuration Item (CI).

    Args:
        ci_id: The identifier of the Configuration Item to update (e.g., "server_001").
        field_to_update: The name of the field to change (e.g., "status").
        new_field_value: The new value for the specified field.
        username: ServiceNow username for authentication (defaults to constant).
        password: ServiceNow password for authentication (defaults to constant).
        servicenow_url: Base URL of the ServiceNow instance (defaults to constant).

    Returns:
        True if the CI field update was simulated successfully, False otherwise.
    """
    flow_logger = get_run_logger()
    flow_logger.info(f"Starting ServiceNow CI Field Update Flow for CI '{ci_id}'...")

    # 1. Authenticate to ServiceNow
    auth_token = simulate_servicenow_authentication(username, password, servicenow_url)
    if not auth_token:
        flow_logger.error("Flow failed: Could not authenticate to ServiceNow.")
        return False

    # 2. Update the specified CI field
    # The `get_ci_data` task is not directly called here to keep the flow focused
    # on just updating, as per the request. In a real scenario, you might call it
    # for validation or to retrieve the sys_id if `ci_id` is not the sys_id.
    update_success = update_ci_field(
        ci_id, field_to_update, new_field_value, auth_token, servicenow_url
    )

    if update_success:
        flow_logger.info(f"Flow completed successfully: CI '{ci_id}' field '{field_to_update}' updated to '{new_field_value}'.")
        return True
    else:
        flow_logger.error(f"Flow failed: Failed to update CI '{ci_id}' field '{field_to_update}'.")
        return False

# --- Deployment Configuration (Optional but good for production style) ---
# This block is used to define how this flow can be deployed to a Prefect server.
# To deploy, save this file (e.g., `servicenow_ci_update_flow.py`) and run:
# prefect deploy servicenow_ci_update_flow.py:servicenow_ci_update_flow_deployment

if __name__ == "__main__":
    # Example local execution
    print("\n--- Running successful update example ---")
    flow_run_result = servicenow_ci_update_flow(
        ci_id="server_001",
        field_to_update="status",
        new_field_value="maintenance"
    )
    print(f"\nFlow run finished. Success: {flow_run_result}")

    print("\n--- Running another successful update example ---")
    flow_run_result_2 = servicenow_ci_update_flow(
        ci_id="database_005",
        field_to_update="owner",
        new_field_value="New DB Ops Team"
    )
    print(f"\nFlow run finished. Success: {flow_run_result_2}")

    print("\n--- Running example simulating an error in update (due to new_field_value) ---")
    flow_run_result_3 = servicenow_ci_update_flow(
        ci_id="router_abc",
        field_to_update="status",
        new_field_value="error_state_simulated" # This value will trigger the simulated error
    )
    print(f"\nFlow run finished. Success: {flow_run_result_3}")

    print("\n--- Running example simulating CI not found/error (due to ci_id) ---")
    flow_run_result_4 = servicenow_ci_update_flow(
        ci_id="non_existent_ci", # This CI ID will trigger the simulated error in update_ci_field
        field_to_update="status",
        new_field_value="operational"
    )
    print(f"\nFlow run finished. Success: {flow_run_result_4}")

    # To create a Prefect deployment for this flow:
    # (Uncomment and run `prefect deploy <filename>`)
    # from datetime import timedelta # Needed for IntervalSchedule if using schedule

    # servicenow_ci_update_flow_deployment = Deployment.build_from_flow(
    #     flow=servicenow_ci_update_flow,
    #     name="servicenow-ci-update-deployment",
    #     version="1.0",
    #     tags=["servicenow", "ci-management"],
    #     parameters={
    #         "ci_id": "server_001",
    #         "field_to_update": "status",
    #         "new_field_value": "operational"
    #     },
    #     description="A flow to update a specific field of a Configuration Item in ServiceNow.",
    #     # schedule=IntervalSchedule(interval=timedelta(days=1)), # Example daily schedule
    #     # infrastructure=DockerContainer(image="your-prefect-image:latest"), # Example infrastructure
    # )
    # servicenow_ci_update_flow_deployment.apply()

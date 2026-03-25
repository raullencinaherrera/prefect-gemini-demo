import asyncio
import uuid
from prefect import flow, task
from pydantic import BaseModel, Field

# --- Device Data Model ---
class Device(BaseModel):
    """Represents a single IoT device with its properties."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Unique identifier for the device")
    name: str = Field(..., description="Human-readable name of the device")
    type: str = Field(..., description="Type of the device (e.g., 'sensor', 'actuator', 'gateway')")
    status: str = Field(default="offline", description="Current operational status of the device")
    location: str | None = Field(None, description="Geographic or logical location of the device")
    firmware_version: str = Field(default="1.0.0", description="Current firmware version")

# --- Prefect Tasks ---
@task(name="Load Device Data", description="Simulates loading device configurations from a backend system.")
async def load_device_configurations(count: int = 10) -> list[Device]:
    """
    Simulates fetching a specified number of device configurations.
    In a real scenario, this would interact with a database, API, or configuration files.

    Args:
        count: The number of devices to simulate loading.

    Returns:
        A list of Device objects.
    """
    print(f"[Task: Load Device Data] Starting to load {count} device configurations...")
    await asyncio.sleep(1.5)  # Simulate network latency or database query time

    devices = []
    for i in range(count):
        device_type = "sensor" if i % 3 == 0 else ("actuator" if i % 3 == 1 else "gateway")
        status = "online" if i % 2 == 0 else "maintenance"
        location = f"Building A, Floor {i % 5 + 1}"
        firmware = f"1.0.{i % 5}"
        
        device = Device(
            name=f"PROD-DEV-{i + 1:03d}",
            type=device_type,
            status=status,
            location=location,
            firmware_version=firmware
        )
        devices.append(device)

    print(f"[Task: Load Device Data] Successfully loaded {len(devices)} device configurations.")
    return devices

@task(name="Validate Device Configuration", description="Performs validation on a single device configuration.")
async def validate_device_configuration(device: Device) -> Device:
    """
    Simulates validating a single device's configuration.
    This might include checking against schemas, business rules, or security policies.

    Args:
        device: The Device object to validate.

    Returns:
        The validated Device object (potentially with updated status).
    """
    print(f"[Task: Validate] Validating device '{device.name}' (ID: {device.id})...")
    await asyncio.sleep(0.3)  # Simulate validation logic

    # Example validation: if a sensor is offline, mark it for re-check
    if device.type == "sensor" and device.status == "offline":
        device.status = "validation_failed_offline"
        print(f"[Task: Validate] Device '{device.name}' (sensor) is offline. Marked for re-check.")
    else:
        print(f"[Task: Validate] Device '{device.name}' configuration validated successfully.")
    
    return device

@task(name="Register Device to Platform", description="Simulates registering a validated device to an IoT platform.")
async def register_device_to_platform(device: Device) -> Device:
    """
    Simulates the process of registering a device to an IoT management platform.
    This could involve API calls, updating a central registry, or provisioning credentials.

    Args:
        device: The Device object to register.

    Returns:
        The Device object with its status updated to 'registered' or similar.
    """
    if device.status.startswith("validation_failed"): # Skip if validation failed
        print(f"[Task: Register] Skipping registration for '{device.name}' due to validation failure.")
        return device

    print(f"[Task: Register] Registering device '{device.name}' (Type: {device.type}) to platform...")
    await asyncio.sleep(0.8)  # Simulate API call to platform

    device.status = "registered"
    print(f"[Task: Register] Device '{device.name}' successfully registered with platform.")
    return device

# --- Prefect Flow ---
@flow(
    name="Enhanced Device Onboarding Flow",
    description="Loads a larger set of device configurations, validates them, and registers them to a platform.",
    version="1.0.0"
)
async def enhanced_device_onboarding_flow(number_of_devices: int = 20): # Increased default device count
    """
    This flow orchestrates the onboarding process for a specified number of IoT devices.
    It simulates loading device configurations, validating each one, and then registering
    them to a central IoT platform. This version handles a larger scale of devices.

    Args:
        number_of_devices: The total number of devices to process in this flow run.
    """
    print(f"\n--- Starting Enhanced Device Onboarding Flow for {number_of_devices} devices ---")

    # Step 1: Load all device configurations
    print("Initiating device configuration loading...")
    device_configs = await load_device_configurations(count=number_of_devices)

    if not device_configs:
        print("No device configurations loaded. Exiting flow.")
        return []

    # Step 2: Concurrently validate each device configuration
    print("Initiating concurrent device configuration validation...")
    validation_futures = [
        validate_device_configuration.submit(device)
        for device in device_configs
    ]
    validated_devices = await asyncio.gather(*validation_futures)
    print(f"Completed validation for {len(validated_devices)} devices.")

    # Step 3: Concurrently register validated devices to the platform
    print("Initiating concurrent device registration...")
    registration_futures = [
        register_device_to_platform.submit(device)
        for device in validated_devices
    ]
    registered_devices = await asyncio.gather(*registration_futures)
    print(f"Completed registration for {len(registered_devices)} devices.")

    print(f"--- Enhanced Device Onboarding Flow finished. Processed {len(registered_devices)} devices ---")

    return registered_devices

# --- Local Execution Entry Point ---
if __name__ == "__main__":
    # To run this flow locally with 20 devices:
    asyncio.run(enhanced_device_onboarding_flow(number_of_devices=20))
    
    # You can change the number of devices to process more:
    # asyncio.run(enhanced_device_onboarding_flow(number_of_devices=50))

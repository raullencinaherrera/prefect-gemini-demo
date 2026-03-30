import asyncio
from datetime import datetime, timezone
import random
from typing import List, Dict, Any

from prefect import flow, task, get_run_logger

# --- Configuration Constants ---
# In a production setup, sensitive information like API keys
# would be managed securely (e.g., Prefect Blocks, environment variables).
FACEBOOK_API_KEY = "DUMMY_FACEBOOK_ACCESS_TOKEN_XYZ123"
FACEBOOK_PAGE_ID = "DUMMY_PAGE_ID_4567"
MAX_MESSAGES_TO_FETCH = 5
SIMULATED_API_LATENCY_SECONDS = 2


# --- Tasks ---

@task
async def authenticate_facebook_api(api_key: str) -> bool:
    """
    Simulates authentication with the Facebook Graph API.
    In a real scenario, this would involve OAuth flows or direct API key validation.
    """
    logger = get_run_logger()
    logger.info("Attempting to authenticate with Facebook API...")
    await asyncio.sleep(SIMULATED_API_LATENCY_SECONDS)  # Simulate network latency

    if api_key and api_key.startswith("DUMMY_"):  # Simple dummy check
        logger.info("Facebook API authentication successful (simulated).")
        return True
    logger.error("Facebook API authentication failed (simulated). Invalid API key.")
    return False

@task
async def fetch_raw_messages_from_facebook(
    api_key: str,
    page_id: str,
    last_n_messages: int = MAX_MESSAGES_TO_FETCH
) -> List[Dict[str, Any]]:
    """
    Simulates fetching raw messages from a Facebook Page inbox using the Graph API.
    Returns a list of dummy message dictionaries.
    """
    logger = get_run_logger()
    logger.info(f"Simulating fetching {last_n_messages} messages for Page ID: {page_id}...")
    await asyncio.sleep(SIMULATED_API_LATENCY_SECONDS * 1.5)  # Simulate API call latency

    if not api_key:
        logger.error("API key is missing for fetching messages.")
        return []

    # Generate dummy messages
    messages = []
    current_time = datetime.now(timezone.utc)
    for i in range(last_n_messages):
        message_id = f"msg_{current_time.strftime('%Y%m%d%H%M%S')}_{i}_{random.randint(100, 999)}"
        sender_id = f"user_{random.choice(['Alice', 'Bob', 'Charlie', 'Diana'])}"
        text_content = random.choice([
            "Hi, I have a question about your service.",
            "What are your operating hours?",
            "Can I get more information about product X?",
            "Thanks for the quick reply!",
            "I'm experiencing an issue with my recent order."
        ])
        # Simulate messages arriving at different times
        message_time = current_time - (i * random.randint(1, 15) * 60 + random.randint(0, 59)) * 60 + random.randint(0, 59)
        message_time_str = message_time.isoformat(timespec='seconds') + 'Z'

        messages.append({
            "id": message_id,
            "from": {"id": sender_id, "name": sender_id.replace('_', ' ').title()},
            "message": text_content,
            "created_time": message_time_str,
            "attachments": []  # Simplified, could include more detail
        })
    
    logger.info(f"Successfully simulated fetching {len(messages)} raw messages.")
    return messages

@task
def process_facebook_messages(raw_messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Processes raw Facebook messages into a more standardized format.
    Adds a 'processed_at' timestamp and normalizes fields.
    This task is synchronous as it primarily involves CPU-bound data transformation.
    """
    logger = get_run_logger()
    if not raw_messages:
        logger.warning("No raw messages to process.")
        return []

    logger.info(f"Processing {len(raw_messages)} raw messages...")
    processed_messages = []
    processed_at = datetime.now(timezone.utc).isoformat(timespec='seconds') + 'Z'

    for msg in raw_messages:
        try:
            processed_messages.append({
                "message_id": msg.get("id"),
                "sender_id": msg.get("from", {}).get("id"),
                "sender_name": msg.get("from", {}).get("name"),
                "content": msg.get("message"),
                "raw_timestamp": msg.get("created_time"),
                "processed_at": processed_at,
                "source": "facebook"
            })
        except Exception as e:
            logger.error(f"Error processing message {msg.get('id', 'N/A')}: {e}")
            # In a real scenario, you might add error handling, send to a dead-letter queue, etc.
            continue
            
    logger.info(f"Successfully processed {len(processed_messages)} messages.")
    return processed_messages

@task
async def store_processed_messages(messages: List[Dict[str, Any]]) -> int:
    """
    Simulates storing processed messages into a data sink (e.g., database, data lake, file system).
    Returns the number of messages successfully 'stored'.
    """
    logger = get_run_logger()
    if not messages:
        logger.warning("No messages to store.")
        return 0

    logger.info(f"Simulating storing {len(messages)} processed messages...")
    await asyncio.sleep(SIMULATED_API_LATENCY_SECONDS)  # Simulate storage I/O latency

    # In a real scenario, this would involve database inserts, API calls to a data lake, etc.
    # For this simulation, we just log a sample of what would be stored.
    for i, msg in enumerate(messages):
        if i < 3: # Log first 3 messages for brevity
            logger.debug(f"Simulated store: {{'message_id': '{msg['message_id']}', 'sender': '{msg['sender_name']}'}}")

    logger.info(f"Successfully simulated storing {len(messages)} messages.")
    return len(messages)


# --- Flow ---

@flow(name="facebook-messages-ingestion-simulation")
async def facebook_messages_simulation_flow(
    api_key: str = FACEBOOK_API_KEY,
    page_id: str = FACEBOOK_PAGE_ID,
    max_messages: int = MAX_MESSAGES_TO_FETCH
):
    """
    A Prefect flow to simulate fetching, processing, and storing messages
    from the Facebook Graph API for a specific page.

    Parameters:
        api_key: The dummy Facebook API key for authentication.
        page_id: The dummy Facebook Page ID to fetch messages from.
        max_messages: The maximum number of dummy messages to simulate fetching.
    """
    logger = get_run_logger()
    logger.info("Starting Facebook messages simulation flow...")

    # 1. Authenticate with Facebook API
    authenticated = await authenticate_facebook_api(api_key=api_key)
    if not authenticated:
        logger.error("Flow terminated due to failed API authentication.")
        return

    # 2. Fetch raw messages from Facebook
    raw_messages = await fetch_raw_messages_from_facebook(
        api_key=api_key,
        page_id=page_id,
        last_n_messages=max_messages
    )

    if not raw_messages:
        logger.info("No new messages found to process. Flow finished.")
        return

    # 3. Process the raw messages
    # This is a synchronous task, but Prefect handles calling it from an async flow.
    processed_messages = process_facebook_messages(raw_messages=raw_messages)

    # 4. Store the processed messages
    stored_count = await store_processed_messages(messages=processed_messages)

    logger.info(f"Facebook messages simulation flow completed. Total messages processed and stored: {stored_count}")

# --- Local Execution ---
if __name__ == "__main__":
    # To run this flow locally, execute the script:
    # python facebook_messages_simulation_flow.py
    #
    # To deploy this flow to a Prefect server:
    # prefect deploy -n facebook-messages-ingestion-simulation facebook_messages_simulation_flow.py:facebook_messages_simulation_flow
    
    asyncio.run(facebook_messages_simulation_flow())

    # Example of running with custom parameters:
    # asyncio.run(facebook_messages_simulation_flow(
    #     api_key="ANOTHER_DUMMY_KEY_XYZ",
    #     page_id="ANOTHER_PAGE_ABC",
    #     max_messages=10
    # ))

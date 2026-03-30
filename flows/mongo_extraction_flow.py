import asyncio
import random
from datetime import datetime, timedelta
from typing import Dict, List, Any

from prefect import flow, task, get_run_logger

# --- Configuration (Simulated) ---
DUMMY_MONGO_CONFIG = {
    "host": "localhost",
    "port": 27017,
    "database": "my_dummy_db",
    "collection": "my_dummy_collection",
    "user": "dummy_user",
    "password": "dummy_password",
}

# --- Tasks ---

@task
async def connect_to_mongo_dummy(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simulates connecting to a MongoDB database.
    In a real scenario, this would establish a client connection using a library like PyMongo.
    """
    logger = get_run_logger()
    logger.info(f"Attempting to 'connect' to MongoDB at {config['host']}:{config['port']}...")
    await asyncio.sleep(random.uniform(0.5, 1.5)) # Simulate network latency and connection time
    
    # Simulate connection success
    connection_details = {
        "status": "connected",
        "client_id": f"dummy_client_{random.randint(1000, 9999)}",
        "database_name": config['database']
    }
    logger.info(f"Successfully 'connected' to database '{config['database']}' (dummy connection ID: {connection_details['client_id']}).")
    return connection_details

@task
async def extract_data_dummy(
    connection: Dict[str, Any], 
    collection_name: str, 
    num_records: int = 100
) -> List[Dict[str, Any]]:
    """
    Simulates extracting data from a MongoDB collection.
    Generates dummy data based on common document structures.
    """
    logger = get_run_logger()
    logger.info(f"Simulating data extraction of {num_records} records from collection '{collection_name}'...")
    await asyncio.sleep(random.uniform(1.0, 3.0)) # Simulate query execution time and data transfer

    dummy_data = []
    for i in range(num_records):
        record = {
            "_id": f"doc_{i+1}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{random.randint(100,999)}",
            "name": f"Item {i+1}",
            "value": round(random.uniform(10.0, 1000.0), 2),
            "category": random.choice(["Electronics", "Books", "Clothing", "HomeGoods", "Food"]),
            "timestamp": (datetime.now() - timedelta(days=random.randint(0, 30), hours=random.randint(0,23), minutes=random.randint(0,59))).isoformat(),
            "is_active": random.choice([True, False]),
            "tags": random.sample(["new", "old", "processed", "urgent", "low_priority", "bestseller"], k=random.randint(1, 3))
        }
        dummy_data.append(record)
    
    logger.info(f"Extracted {len(dummy_data)} dummy records from '{collection_name}'.")
    return dummy_data

@task
async def process_extracted_data_dummy(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Simulates light processing/transformation of the extracted data.
    Adds a 'processed_at' timestamp and a 'status' field based on 'is_active'.
    """
    logger = get_run_logger()
    logger.info(f"Processing {len(raw_data)} records...")
    await asyncio.sleep(random.uniform(0.1, 0.8)) # Simulate CPU-bound processing time

    processed_data = []
    for record in raw_data:
        record["processed_at"] = datetime.now().isoformat()
        record["status"] = "active" if record["is_active"] else "inactive"
        # Example transformation: convert value to integer if it's high
        if record["value"] > 500:
            record["value_tier"] = "high"
        else:
            record["value_tier"] = "low"
        processed_data.append(record)
    
    logger.info(f"Finished processing {len(processed_data)} records.")
    return processed_data

@task
async def persist_data_dummy(data: List[Dict[str, Any]], target_location: str = "dummy_data_lake_path") -> None:
    """
    Simulates persisting the processed data to a target location (e.g., S3, data warehouse, file system).
    """
    logger = get_run_logger()
    logger.info(f"Simulating persistence of {len(data)} records to '{target_location}'...")
    await asyncio.sleep(random.uniform(0.8, 2.0)) # Simulate write time to external storage

    # In a real scenario, this would write to a file (e.g., Parquet, CSV), database table, etc.
    # For this dummy, we just log a small sample to show completion.
    if data:
        sample_record = data[0]
        logger.debug(f"Sample of 'persisted' data: {sample_record}")
    
    logger.info(f"Successfully 'persisted' {len(data)} records to '{target_location}'.")

# --- Flow ---

@flow(name="mongo-data-extraction-dummy-flow", description="A dummy Prefect flow simulating data extraction, processing, and persistence from a MongoDB database.")
async def mongo_extraction_flow(
    config: Dict[str, Any] = DUMMY_MONGO_CONFIG,
    num_records_to_extract: int = 50
) -> None:
    """
    Orchestrates the dummy MongoDB data extraction, processing, and persistence workflow.

    Args:
        config: A dictionary containing dummy MongoDB connection details.
        num_records_to_extract: The number of dummy records to simulate extracting.
    """
    logger = get_run_logger()
    logger.info("Starting dummy MongoDB data extraction flow...")

    # 1. Simulate connection to MongoDB
    mongo_connection = await connect_to_mongo_dummy(config)
    
    # 2. Simulate data extraction from a specific collection
    raw_extracted_data = await extract_data_dummy(
        connection=mongo_connection, 
        collection_name=config["collection"], 
        num_records=num_records_to_extract
    )
    
    # 3. Simulate processing of the extracted data
    processed_data = await process_extracted_data_dummy(raw_data=raw_extracted_data)
    
    # 4. Simulate persistence of the processed data to a target system
    await persist_data_dummy(data=processed_data, target_location="dummy_parquet_lake")
    
    logger.info("Dummy MongoDB data extraction flow completed successfully!")

# --- Main execution block ---
if __name__ == "__main__":
    # Example of how to run the flow locally
    # You can adjust num_records_to_extract here for different test runs
    asyncio.run(mongo_extraction_flow(num_records_to_extract=75))
    # To run with default parameters:
    # asyncio.run(mongo_extraction_flow())

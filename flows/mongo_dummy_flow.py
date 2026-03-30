from prefect import flow, task
import asyncio
from datetime import datetime
import random


class DummyMongoClient:
    """
    A dummy class to simulate a MongoDB client for demonstration purposes.
    It provides basic connect, disconnect, find_one, and insert_one methods.
    """
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.is_connected = False
        print(f"DummyMongoClient: Initialized for {self.connection_string}")

    async def connect(self):
        """Simulates establishing a connection to MongoDB."""
        if not self.is_connected:
            print(f"DummyMongoClient: Simulating connection to {self.connection_string}...")
            await asyncio.sleep(1.0)  # Simulate network latency
            self.is_connected = True
            print("DummyMongoClient: Connection established.")
        else:
            print("DummyMongoClient: Already connected.")

    async def disconnect(self):
        """Simulates closing the connection to MongoDB."""
        if self.is_connected:
            print("DummyMongoClient: Simulating disconnection...")
            await asyncio.sleep(0.5)  # Simulate closing time
            self.is_connected = False
            print("DummyMongoClient: Disconnected.")
        else:
            print("DummyMongoClient: Not connected, no need to disconnect.")

    async def find_one(self, collection_name: str, query: dict) -> dict | None:
        """Simulates finding a single document in a collection."""
        if not self.is_connected:
            raise ConnectionError("DummyMongoClient: Not connected. Call connect() first.")
        print(f"DummyMongoClient: Simulating find_one in '{collection_name}' with query: {query}")
        await asyncio.sleep(0.8) # Simulate query execution time
        if random.random() > 0.3:  # Simulate finding data about 70% of the time
            return {
                "_id": str(random.randint(10000, 99999)),
                "status": "found",
                "query_matched": query,
                "timestamp": datetime.now().isoformat(),
                "data_value": random.randint(1, 1000)
            }
        return None

    async def insert_one(self, collection_name: str, document: dict) -> dict:
        """Simulates inserting a single document into a collection."""
        if not self.is_connected:
            raise ConnectionError("DummyMongoClient: Not connected. Call connect() first.")
        print(f"DummyMongoClient: Simulating insert_one into '{collection_name}' with document: {document}")
        await asyncio.sleep(0.7) # Simulate insert execution time
        return {"acknowledged": True, "inserted_id": str(random.randint(10000, 99999))}


@task
async def connect_to_mongo(connection_string: str) -> DummyMongoClient:
    """
    Task to simulate connecting to a MongoDB instance.
    Returns a dummy client object.
    """
    client = DummyMongoClient(connection_string)
    await client.connect()
    return client


@task
async def perform_dummy_mongo_operation(
    client: DummyMongoClient, operation_type: str, collection_name: str, data: dict = None
):
    """
    Task to simulate performing a MongoDB operation (find or insert).
    """
    if operation_type == "find":
        result = await client.find_one(collection_name, data or {})
        if result:
            print(f"Task 'perform_dummy_mongo_operation': Successfully found document: {result}")
            return result
        else:
            print(f"Task 'perform_dummy_mongo_operation': No document found for query: {data}")
            return None
    elif operation_type == "insert":
        if not data:
            raise ValueError("Data must be provided for insert operation.")
        result = await client.insert_one(collection_name, data)
        print(f"Task 'perform_dummy_mongo_operation': Successfully inserted document. Result: {result}")
        return result
    else:
        raise ValueError(f"Unsupported operation type: {operation_type}")


@task
async def disconnect_from_mongo(client: DummyMongoClient):
    """
    Task to simulate disconnecting from a MongoDB instance.
    """
    await client.disconnect()


@flow(name="mongo-dummy-connection-flow", description="A dummy flow simulating MongoDB connection and operations.")
async def mongo_dummy_flow(
    mongo_connection_string: str = "mongodb://localhost:27017/dummy_db",
    target_collection: str = "my_dummy_collection"
):
    """
    This flow simulates the process of connecting to a MongoDB database,
    performing a dummy find operation, a dummy insert operation,
    and then disconnecting. Error handling ensures disconnection even on failure.
    """
    print(f"Flow '{mongo_dummy_flow.name}': Starting with connection string: {mongo_connection_string}")

    mongo_client = None
    try:
        # 1. Connect to MongoDB
        mongo_client = await connect_to_mongo(mongo_connection_string)

        # 2. Simulate a find operation
        print(f"Flow: Simulating finding a document in '{target_collection}'...")
        find_query = {"item_id": "product_123"}
        found_data = await perform_dummy_mongo_operation(
            client=mongo_client,
            operation_type="find",
            collection_name=target_collection,
            data=find_query
        )
        if found_data:
            print(f"Flow: Received from find operation: {found_data}")
        else:
            print("Flow: No data found in the find operation.")

        # 3. Simulate an insert operation
        print(f"Flow: Simulating inserting a document into '{target_collection}'...")
        new_document = {
            "item_id": f"product_{random.randint(1, 1000)}",
            "quantity": random.randint(10, 500),
            "status": "new",
            "created_at": datetime.now().isoformat()
        }
        insert_result = await perform_dummy_mongo_operation(
            client=mongo_client,
            operation_type="insert",
            collection_name=target_collection,
            data=new_document
        )
        print(f"Flow: Received from insert operation: {insert_result}")

    except Exception as e:
        print(f"Flow '{mongo_dummy_flow.name}': An unexpected error occurred: {e}")
        # Re-raise the exception to mark the flow as failed in Prefect UI
        raise
    finally:
        # 4. Ensure disconnection happens even if operations fail
        if mongo_client:
            await disconnect_from_mongo(mongo_client)
            
    print(f"Flow '{mongo_dummy_flow.name}': Completed.")


if __name__ == "__main__":
    # To run this flow:
    # 1. Ensure you have Prefect installed (`pip install prefect`)
    # 2. Save this file as `mongo_dummy_flow.py`
    # 3. Run from your terminal: `python mongo_dummy_flow.py`
    #    or for deployment: `prefect deploy` (after creating a `prefect.yaml`)

    asyncio.run(mongo_dummy_flow())

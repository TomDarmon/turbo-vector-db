import asyncio
import random
import sys
import os

# Add the SDK to the path so we can import it directly
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "sdk")))

from turbo_vector_sdk import ApiClient, Configuration
from turbo_vector_sdk.api.default_api import DefaultApi
from turbo_vector_sdk.models.create_collection_request import CreateCollectionRequest
from turbo_vector_sdk.models.upsert_request import UpsertRequest
from turbo_vector_sdk.models.upsert_vector import UpsertVector
from turbo_vector_sdk.models.query_request import QueryRequest
from turbo_vector_sdk.models.metric import Metric

async def main():
    # Configure the API client
    configuration = Configuration(host="http://localhost:8080")
    
    async with ApiClient(configuration) as api_client:
        api = DefaultApi(api_client)
        
        try:
            # 1. Check health
            print("Checking health...")
            health = await api.health()
            print(f"Health status: {health.status}")
            
            # # 2. Create a collection
            collection_name = f"test-collection"
            # print(f"\nCreating collection: {collection_name}")
            # create_req = CreateCollectionRequest(
            #     name=collection_name,
            #     dimension=3,
            #     metric=Metric.COSINE
            # )
            # await api.create_collection(create_req)
            # print("Collection created successfully.")
            
            # # 3. Upsert a vector
            # print("\nUpserting vector...")
            # vector_id = "vec1"
            # vector_values = [0.1, 0.2, 0.3]
            # upsert_req = UpsertRequest(
            #     vectors=[
            #         UpsertVector(
            #             id=vector_id,
            #             values=vector_values,
            #             metadata={"category": "test"}
            #         )
            #     ]
            # )
            # upsert_res = await api.upsert_vectors(collection_name, upsert_req)
            # operation_id = getattr(upsert_res.actual_instance, "operation_id", None)
            # print(f"Upsert accepted. Operation ID: {operation_id}")

            # if operation_id:
            #     print("Waiting for operation to be applied...")
            #     for _ in range(50):
            #         status = await api.get_operation_status(collection_name, operation_id)
            #         if status.status.value == "applied":
            #             print(f"Operation applied at generation {status.generation}.")
            #             break
            #         await asyncio.sleep(0.1)
            #     else:
            #         raise RuntimeError(
            #             f"Timed out waiting for operation {operation_id} to become applied."
            #         )
            
            # 4. Query the vector
            print("\nQuerying vector...")
            query_req = QueryRequest(
                vector=[0.1, 0.2, 0.3],
                top_k=1,
                include_metadata=True,
                include_values=True
            )
            results = await api.query_vectors(collection_name, query_req)
            
            print("\nQuery results:")
            if results.matches:
                for match in results.matches:
                    print(f"  - ID: {match.id}")
                    print(f"    Score: {match.score}")
                    print(f"    Metadata: {match.metadata}")
                    print(f"    Values: {match.values}")
            else:
                print("No matches found.")

            # 5. Clean up (Optional)
            # print(f"\nDeleting collection: {collection_name}")
            # await api.delete_collection(collection_name)
            # print("Collection deleted.")

        except Exception as e:
            print(f"Error during execution: {e}")

if __name__ == "__main__":
    asyncio.run(main())

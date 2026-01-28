import os
import shutil
from pyspark.sql import DataFrame

try:
    from azure.storage.blob import BlobServiceClient
except ImportError:
    print("[!] ERROR: azure-storage-blob is missing. Please install the package to enable cloud loading.")

def save_to_azurite(df: DataFrame, metric_name: str, config: dict) -> None:
    """
    Orchestrates the persistence of Spark DataFrames by staging them locally and 
    synchronizing with Azurite storage. Normalizes Spark-generated partition files 
    into a static 'data.parquet' for stable downstream consumption in BI tools.
    """
    # Resolve absolute paths for staging and project root
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))
    staging_dir = os.path.join(project_root, "data", "staging", metric_name)
    
    # Extract storage metadata and credentials from configuration
    storage_conf = config.get('storage', {})
    conn_str = storage_conf.get('connection_string')
    container_name = storage_conf.get('container_name')

    print(f"      [Loader] Local staging initiated: {staging_dir}")

    # Synchronize local file system state for current metric
    if os.path.exists(staging_dir):
        shutil.rmtree(staging_dir)
    
    # Persist DataFrame to staging; coalesce(1) ensures a single part file for stable blob mapping
    df.coalesce(1).write.mode("overwrite").parquet(staging_dir)

    print(f"      [Loader] Initiating cloud synchronization to: {container_name}")
    
    try:
        # Initialize client with API version compatible with local Azurite emulator
        blob_service_client = BlobServiceClient.from_connection_string(conn_str, api_version="2021-08-06")
        container_client = blob_service_client.get_container_client(container_name)

        # Ensure idempotency by purging existing blobs under the current metric prefix
        blobs_to_delete = container_client.list_blobs(name_starts_with=f"{metric_name}/")
        for blob in blobs_to_delete:
            container_client.delete_blob(blob.name)

        # Traverse staging directory and synchronize parquet assets
        for root, dirs, files in os.walk(staging_dir):
            for file in files:
                if file.endswith(".parquet"):
                    local_file_path = os.path.join(root, file)
                    
                    # Enforce static naming convention to provide a stable URI for BI connectors
                    blob_name = f"{metric_name}/data.parquet"
                    
                    with open(local_file_path, "rb") as data:
                        container_client.upload_blob(name=blob_name, data=data, overwrite=True)
                        
        print(f"      [Loader] Cloud synchronization successful.")
        
    except Exception as e:
        print(f"      [Loader] Critical failure during cloud synchronization: {e}")
        raise e
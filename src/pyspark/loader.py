import os
import shutil
from pyspark.sql import DataFrame

try:
    from azure.storage.blob import BlobServiceClient
except ImportError:
    print("[!] ERROR: azure-storage-blob missing. Run: pip install azure-storage-blob")

def save_to_azurite(df: DataFrame, metric_name: str, config: dict) -> None:
    """
    Saves DataFrame to local staging and then uploads to Azurite using Python SDK.
    RENAMES the random Spark parquet file to a static name ('data.parquet') for easy Power BI access.
    
    :param df: Spark DataFrame to save
    :param metric_name: Name of the metric (used for folder naming)
    :param config: Configuration dictionary containing storage details
    """
    # 1. Setup paths
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))
    staging_dir = os.path.join(project_root, "data", "staging", metric_name)
    
    # 2. Get Azure config
    storage_conf = config.get('storage', {})
    conn_str = storage_conf.get('connection_string')
    container_name = storage_conf.get('container_name')

    print(f"      [Loader] Staging data locally to: {staging_dir}")

    # 3. Write locally with Spark
    if os.path.exists(staging_dir):
        shutil.rmtree(staging_dir)
    
    # Save as Parquet locally (Spark creates part-00000... files)
    df.coalesce(1).write.mode("overwrite").parquet(staging_dir)

    # 4. Upload to Azurite (with RENAME)
    print(f"      [Loader] Preparing upload to container: {container_name}")
    
    try:
        blob_service_client = BlobServiceClient.from_connection_string(conn_str, api_version="2021-08-06")
        container_client = blob_service_client.get_container_client(container_name)

        # CLEANUP: Remove old blobs to avoid mixing
        # This ensures we start with a clean slate for this specific metric folder
        blobs_to_delete = container_client.list_blobs(name_starts_with=f"{metric_name}/")
        for blob in blobs_to_delete:
            container_client.delete_blob(blob.name)

        # Upload with STATIC NAME
        for root, dirs, files in os.walk(staging_dir):
            for file in files:
                if file.endswith(".parquet"):
                    local_file_path = os.path.join(root, file)
                    
                    # KEY CHANGE: Instead of keeping the random 'part-xyz.parquet' name,
                    # we upload it as 'data.parquet'. This gives us a stable URL for Power BI.
                    blob_name = f"{metric_name}/data.parquet"
                    
                    # print(f"      [Loader] Uploading file as: {blob_name}")
                    with open(local_file_path, "rb") as data:
                        container_client.upload_blob(name=blob_name, data=data, overwrite=True)
                        
        print(f"      [Loader] Upload success.")
        
    except Exception as e:
        print(f"      [Loader] Upload ERROR: {e}")
        raise e
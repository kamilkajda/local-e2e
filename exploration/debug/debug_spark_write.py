import os
import sys
from pyspark.sql import SparkSession

# Setup paths to reuse existing modules
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(os.path.join(project_root, 'src', 'pyspark'))

from spark_setup import _configure_windows_environment

def debug_write():
    print("--- DEBUG: Native Spark Write to Azurite ---")
    
    # 1. Setup WinUtils
    _configure_windows_environment()
    
    # 2. Hardcoded Config for Debugging
    account_name = "devstoreaccount1"
    account_key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
    # NOTE: Using 127.0.0.1 if running on Laptop. Change to 192.168.0.218 if on PC.
    blob_endpoint = "http://127.0.0.1:10000" 
    container = "debug-container"

    print(f"Endpoint: {blob_endpoint}")

    # 3. Init Spark with specific hadoop-azure version
    spark = SparkSession.builder \
        .appName("DebugWrite") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4") \
        .getOrCreate()

    # 4. Configuration Injection
    spark.conf.set(f"fs.azure.account.auth.type.{account_name}.blob.core.windows.net", "SharedKey")
    spark.conf.set(f"fs.azure.account.key.{account_name}.blob.core.windows.net", account_key)
    spark.conf.set(f"fs.azure.storage.blob.endpoint.{account_name}.blob.core.windows.net", blob_endpoint)
    
    # Security tweaks for Azurite
    spark.conf.set("fs.azure.secure.mode", "false")
    spark.conf.set("fs.azure.always.use.https", "false")
    spark.conf.set("fs.azure.io.compression.enabled", "false")

    # 5. Create Data
    data = [("Test", 1), ("Success", 2)]
    df = spark.createDataFrame(data, ["col1", "col2"])

    # 6. Attempt Write
    output_path = f"wasb://{container}@{account_name}.blob.core.windows.net/test_data"
    print(f"Writing to: {output_path}")

    try:
        df.write.mode("overwrite").parquet(output_path)
        print("\n[SUCCESS] Write completed without error!")
    except Exception as e:
        print(f"\n[FAILURE] Write error: {e}")

    spark.stop()

if __name__ == "__main__":
    debug_write()
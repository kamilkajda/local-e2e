import sys
import os
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

# Requirement: azure-storage-blob to generate SAS token
try:
    from azure.storage.blob import generate_account_sas, ResourceTypes, AccountSasPermissions
except ImportError:
    print("[!] ERROR: azure-storage-blob is missing. Run: pip install azure-storage-blob")
    sys.exit(1)

# Add src/pyspark to system path to reuse ONLY the windows environment fix
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(os.path.join(project_root, 'src', 'pyspark'))

try:
    # We only import the Windows environment fix, NOT the session creator
    from spark_setup import _configure_windows_environment
except ImportError as e:
    print(f"Error importing spark_setup: {e}")
    sys.exit(1)

def create_debug_session_with_sas():
    """
    Creates a standalone Spark session configured with SAS Token instead of Shared Key.
    This bypasses signature validation issues often seen with Azurite.
    """
    print("   [Debug] Initializing Spark with SAS Token configuration...")
    
    # 1. Setup Windows Environment (Hadoop Home)
    _configure_windows_environment()
    
    # 2. Configuration for Azurite
    account_name = "devstoreaccount1"
    account_key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
    # Localhost for this debug script is fine
    blob_endpoint = f"http://127.0.0.1:10000/{account_name}"
    container_name = "connection-test-pc"

    # 3. Generate SAS Token (The Fix)
    # SAS tokens are less strict about HTTP signatures than Shared Key auth
    sas_token = generate_account_sas(
        account_name=account_name,
        account_key=account_key,
        resource_types=ResourceTypes(service=True, container=True, object=True),
        permission=AccountSasPermissions(read=True, write=True, delete=True, list=True, add=True, create=True, update=True, process=True),
        expiry=datetime.utcnow() + timedelta(hours=1)
    )
    
    # 4. Build Spark Session
    spark = SparkSession.builder \
        .appName("Debug_Azurite_SAS") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4") \
        .getOrCreate()
    
    # 5. Apply Configuration
    # Use SAS token for the specific container
    spark.conf.set(f"fs.azure.sas.{container_name}.{account_name}.blob.core.windows.net", sas_token)
    
    # Map the endpoint
    spark.conf.set(f"fs.azure.storage.blob.endpoint.{account_name}.blob.core.windows.net", blob_endpoint)
    
    # Disable security for local HTTP
    spark.conf.set("fs.azure.secure.mode", "false")
    spark.conf.set("fs.azure.always.use.https", "false")
    
    return spark, container_name, account_name

def run_debug_write():
    print("--- DEBUG: Spark Write to Azurite (SAS Method) ---")
    
    try:
        spark, container, account = create_debug_session_with_sas()
        
        # Create simple DataFrame
        data = [("A", 100), ("B", 200), ("C", 300)]
        df = spark.createDataFrame(data, ["letter", "value"])
        
        # Attempt Write
        # Note: When using SAS, we still use the standard WASB URL structure
        output_path = f"wasb://{container}@{account}.blob.core.windows.net/sas_test_file"
        print(f"   [Debug] Attempting to write to: {output_path}")
        
        df.write.mode("overwrite").parquet(output_path)
        
        print("\n[SUCCESS] Write operation completed!")
        print("SAS Token authentication worked. You should see 'sas_test_file' in Azurite.")
        
    except Exception as e:
        print(f"\n[ERROR] Write failed: {e}")
        # import traceback
        # traceback.print_exc()
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    run_debug_write()
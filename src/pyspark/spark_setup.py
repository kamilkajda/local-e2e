import os
import sys
from pyspark.sql import SparkSession

def _parse_connection_string(conn_str):
    """Parses Azurite connection string and returns a config dictionary."""
    config = {}
    parts = conn_str.split(';')
    for part in parts:
        if part:
            try:
                key, value = part.split('=', 1)
                config[key] = value
            except ValueError:
                pass
    return config

def _configure_windows_environment():
    """
    Sets up HADOOP_HOME and PATH for Windows specifically using local binaries.
    This makes the project portable without requiring global env vars.
    """
    # Run this configuration only on Windows
    if sys.platform != 'win32':
        return

    # Locate the 'hadoop' folder in the project root
    # Assumption: structure is local-e2e/src/pyspark/spark_setup.py -> go up 2 levels
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))
    
    # Construct path to local-e2e/hadoop/bin
    hadoop_home = os.path.join(project_root, "hadoop")
    hadoop_bin = os.path.join(hadoop_home, "bin")
    
    winutils_path = os.path.join(hadoop_bin, "winutils.exe")
    hadoop_dll_path = os.path.join(hadoop_bin, "hadoop.dll")

    # Check if the required binaries actually exist
    if os.path.exists(winutils_path) and os.path.exists(hadoop_dll_path):
        print(f"   [SparkSetup] Found local WinUtils at: {hadoop_bin}")
        
        # 1. Set HADOOP_HOME environment variable
        os.environ['HADOOP_HOME'] = hadoop_home
        
        # 2. Add the bin folder to PATH (critical for loading hadoop.dll)
        current_path = os.environ.get('PATH', '')
        if hadoop_bin not in current_path:
            os.environ['PATH'] = hadoop_bin + os.pathsep + current_path
            print("   [SparkSetup] Added Hadoop bin to PATH.")
    else:
        print("   [SparkSetup] WARNING: WinUtils not found in project!")
        print(f"   Expected path: {hadoop_bin}")
        print("   Please download hadoop.dll and winutils.exe to this folder to fix write errors.")

def create_spark_session(config):
    """Initializes and configures SparkSession."""
    
    print("   [SparkSetup] Initializing SparkSession...")

    # FIX 1: "Python not found" error (Microsoft Store issue)
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    # FIX 2: "UnsatisfiedLinkError" (Missing hadoop.dll on Windows)
    _configure_windows_environment()

    storage_config = config.get('storage', {})
    conn_str = storage_config.get('connection_string')
    env = config.get('env')
    
    builder = SparkSession.builder.appName(f"GusETL_{env}")

    if conn_str and env == 'dev':
        print("   [SparkSetup] Configuring for local Azurite...")
        
        conn_parts = _parse_connection_string(conn_str)
        account_name = conn_parts.get('AccountName', 'devstoreaccount1')
        account_key = conn_parts.get('AccountKey')
        blob_endpoint = conn_parts.get('BlobEndpoint')
        
        if not account_key or not blob_endpoint:
             print("   [ERROR] Invalid Connection String in settings.json")
             sys.exit(20)

        # Drivers
        # hadoop-azure 3.3.4 matches our WinUtils version
        spark_jars_packages = "org.apache.hadoop:hadoop-azure:3.3.4"
        
        builder = builder.config("spark.jars.packages", spark_jars_packages)
        
        spark = builder.getOrCreate()

        # Auth - Shared Key
        spark.conf.set(f"fs.azure.account.auth.type.{account_name}.blob.core.windows.net", "SharedKey")
        spark.conf.set(f"fs.azure.account.key.{account_name}.blob.core.windows.net", account_key)
        
        # Endpoint Mapping logic
        blob_endpoint_host = blob_endpoint
        if f"/{account_name}" in blob_endpoint:
             blob_endpoint_host = blob_endpoint.replace(f"/{account_name}", "")
        
        if blob_endpoint_host.endswith("/"):
            blob_endpoint_host = blob_endpoint_host[:-1]

        spark.conf.set(f"fs.azure.storage.blob.endpoint.{account_name}.blob.core.windows.net", blob_endpoint_host)
        
        # Security settings for HTTP/Emulator
        spark.conf.set("fs.azure.secure.mode", "false")
        spark.conf.set("fs.azure.always.use.https", "false")
        spark.conf.set("fs.azure.io.compression.enabled", "false")
        spark.conf.set("fs.azure.project.id", "local_etl")
        
    else:
        spark = builder.getOrCreate()

    # FIX: Reduce logging verbosity to clean up terminal (INFO logs hidden)
    spark.sparkContext.setLogLevel("ERROR")
    
    print("   [SparkSetup] SparkSession created successfully.")
    return spark
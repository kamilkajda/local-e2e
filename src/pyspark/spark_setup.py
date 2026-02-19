import os
import sys

from pyspark.sql import SparkSession


def _parse_connection_string(conn_str):
    """
    Parses an Azure Storage connection string into a key-value dictionary.
    Used to extract credentials and endpoints for Hadoop configuration.
    """
    config = {}
    parts = conn_str.split(";")
    for part in parts:
        if part:
            try:
                key, value = part.split("=", 1)
                config[key] = value
            except ValueError:
                pass
    return config


def _configure_windows_environment():
    """
    Normalizes the Windows environment by dynamically resolving HADOOP_HOME
    and injecting local binaries into the system PATH. Ensures portability
    across different local development machines without manual environment variables.
    """
    if sys.platform != "win32":
        return

    # Dynamic project root resolution relative to script location
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))

    hadoop_home = os.path.join(project_root, "hadoop")
    hadoop_bin = os.path.join(hadoop_home, "bin")

    winutils_path = os.path.join(hadoop_bin, "winutils.exe")
    hadoop_dll_path = os.path.join(hadoop_bin, "hadoop.dll")

    if os.path.exists(winutils_path) and os.path.exists(hadoop_dll_path):
        os.environ["HADOOP_HOME"] = hadoop_home

        # Inject hadoop binaries into PATH to enable native library loading (hadoop.dll)
        current_path = os.environ.get("PATH", "")
        if hadoop_bin not in current_path:
            os.environ["PATH"] = hadoop_bin + os.pathsep + current_path
    else:
        print(f"   [SparkSetup] WARNING: WinUtils binaries missing at {hadoop_bin}")


def create_spark_session(config):
    """
    Configures and initializes a SparkSession. Implements specific logic for
    WASB filesystem abstraction when targeting local Azurite storage.
    """
    print("   [SparkSetup] Initializing SparkSession instance...")

    # Environmental normalization for Python workers
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    _configure_windows_environment()

    storage_config = config.get("storage", {})
    conn_str = storage_config.get("connection_string")
    env = config.get("env")

    builder = SparkSession.builder.appName(f"GusETL_{env}")

    if conn_str and env == "dev":
        print("   [SparkSetup] Applying Azurite storage driver configuration...")

        conn_parts = _parse_connection_string(conn_str)
        account_name = conn_parts.get("AccountName", "devstoreaccount1")
        account_key = conn_parts.get("AccountKey")
        blob_endpoint = conn_parts.get("BlobEndpoint")

        if not account_key or not blob_endpoint:
            print("   [ERROR] Malformed connection string in configuration.")
            sys.exit(20)

        # Storage connector artifacts
        spark_jars_packages = "org.apache.hadoop:hadoop-azure:3.3.4"
        builder = builder.config("spark.jars.packages", spark_jars_packages)

        spark = builder.getOrCreate()

        # Hadoop FileSystem configuration for Azure Blob Storage (WASB)
        spark.conf.set(
            f"fs.azure.account.auth.type.{account_name}.blob.core.windows.net",
            "SharedKey",
        )
        spark.conf.set(f"fs.azure.account.key.{account_name}.blob.core.windows.net", account_key)

        # Endpoint mapping for local emulator
        blob_endpoint_host = blob_endpoint
        if f"/{account_name}" in blob_endpoint:
            blob_endpoint_host = blob_endpoint.replace(f"/{account_name}", "")

        if blob_endpoint_host.endswith("/"):
            blob_endpoint_host = blob_endpoint_host[:-1]

        spark.conf.set(
            f"fs.azure.storage.blob.endpoint.{account_name}.blob.core.windows.net",
            blob_endpoint_host,
        )

        # Security overrides for non-SSL local development
        spark.conf.set("fs.azure.secure.mode", "false")
        spark.conf.set("fs.azure.always.use.https", "false")
        spark.conf.set("fs.azure.io.compression.enabled", "false")
        spark.conf.set("fs.azure.project.id", "local_etl")

    else:
        spark = builder.getOrCreate()

    # Suppress verbose logging for production-like console output
    spark.sparkContext.setLogLevel("ERROR")

    print("   [SparkSetup] SparkSession orchestration complete.")
    return spark

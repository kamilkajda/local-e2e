import os
import sys

from pyspark.sql import SparkSession

# Add src/pyspark to system path to reuse existing setup modules
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(os.path.join(project_root, "src", "pyspark"))

try:
    from spark_setup import create_spark_session

    from utils import load_config
except ImportError as e:
    print(f"Error importing modules: {e}")
    sys.exit(1)


def run_native_write_test():
    print("--- TESTING NATIVE SPARK WRITE (WASB) ---")

    # 1. Load Config
    config_path = os.path.join(project_root, "configs", "dev", "settings.json")
    if not os.path.exists(config_path):
        print(f"Config not found: {config_path}")
        return

    config = load_config(config_path)

    # 2. Init Spark
    # This uses spark_setup.py which sets up WinUtils and Azurite auth (SharedKey)
    spark = create_spark_session(config)
    spark.sparkContext.setLogLevel("ERROR")

    # 3. Create Test Data
    print("\n>>> Creating test DataFrame...")
    data = [("Native", 1), ("Spark", 2), ("Write", 3)]
    df = spark.createDataFrame(data, ["col_string", "col_int"])
    df.show()

    # 4. Prepare Path
    storage_conf = config.get("storage", {})
    container_name = "connection-test-pc"  # Use test container to avoid messing up ETL data
    # Extract account from connection string or hardcode for dev
    account_name = "devstoreaccount1"

    # Native WASB path
    output_path = f"wasb://{container_name}@{account_name}.blob.core.windows.net/native_write_test"

    print(f"\n>>> Attempting NATIVE write to: {output_path}")
    print("    If this works, we don't need loader.py anymore.")

    try:
        # Try writing directly with Spark
        df.write.mode("overwrite").parquet(output_path)
        print("\n[SUCCESS] Native Spark write succeeded!")

    except Exception as e:
        print(f"\n[FAILURE] Native Spark write failed.")
        print(f"Error: {e}")

    spark.stop()


if __name__ == "__main__":
    run_native_write_test()

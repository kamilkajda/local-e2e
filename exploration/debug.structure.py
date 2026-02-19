import glob
import os
import sys

from pyspark.sql import functions as F

# Add src/pyspark to system path to reuse existing setup modules
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(os.path.join(project_root, "src", "pyspark"))

try:
    from spark_setup import create_spark_session
except ImportError as e:
    print(f"Error importing modules: {e}")
    sys.exit(1)


def find_any_json_file(base_path):
    """
    Recursively searches for the first .json file in the directory structure.
    """
    for root, dirs, files in os.walk(base_path):
        for file in files:
            if file.endswith(".json"):
                return os.path.join(root, file)
    return None


def inspect_with_spark():
    """
    Loads any available raw JSON file using PySpark to visualize Schema.
    """
    # Initialize minimal local session
    config = {"env": "dev"}
    spark = create_spark_session(config)
    spark.sparkContext.setLogLevel("ERROR")

    # Path to raw data
    raw_data_path = os.path.join(project_root, "data", "raw")

    print(f"\n--- Scanning for data in: {raw_data_path} ---")
    target_file = find_any_json_file(raw_data_path)

    if not target_file:
        print(f"[!] No JSON files found in {raw_data_path}. Run ETL Extract first.")
        spark.stop()
        return

    print(f">>> INSPECTING FOUND FILE: {target_file}")

    try:
        # 1. Read JSON with multiLine option (required for standard API responses)
        # Allows reading JSONs that span multiple lines or are single array objects
        df = spark.read.option("multiLine", True).json(target_file)

        print("\n--- [1] Raw Schema (Inferred by Spark) ---")
        df.printSchema()

        # 2. Show raw root structure
        print("\n--- [2] Root Data Sample ---")
        df.show(truncate=False)

        # 3. Simulate first level of explosion (Results Array)
        # This confirms if the file structure matches GUS standard (root -> results[])
        if "results" in df.columns:
            print("\n--- [3] Exploding 'results' array (Units) ---")
            df_units = df.select(F.explode(F.col("results")).alias("unit"))
            df_units.printSchema()

            print("\nSample Unit Data:")
            df_units.select("unit.id", "unit.name").show(5, truncate=False)

            # 4. Simulate second level of explosion (Values Array)
            print("\n--- [4] Exploding 'unit.values' array (Years & Vals) ---")
            df_values = df_units.select(
                F.col("unit.name"), F.explode(F.col("unit.values")).alias("val_obj")
            )
            df_values.printSchema()

            print("\nFinal Flattened Data Sample:")
            df_values.select("name", F.col("val_obj.year"), F.col("val_obj.val")).show(5)
        else:
            print("\n[!] 'results' field not found. This might not be a standard GUS BDL file.")

    except Exception as e:
        print(f"\n[ERROR] Failed to inspect file: {e}")

    spark.stop()


if __name__ == "__main__":
    inspect_with_spark()

import sys
import os
import shutil
from pyspark.sql import SparkSession

# --- DYNAMIC PATH SETUP ---
# Detect if running from root 'exploration' or subfolder 'exploration/debug'
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)

if parent_dir_name in ['debug', 'tools', 'playground']:
    # We are inside a subfolder, go up 2 levels to project root
    project_root = os.path.dirname(os.path.dirname(current_dir))
else:
    # We are directly in exploration, go up 1 level
    project_root = os.path.dirname(current_dir)

sys.path.append(os.path.join(project_root, 'src', 'pyspark'))

try:
    from spark_setup import create_spark_session
except ImportError as e:
    print(f"Error importing modules: {e}")
    print(f"Computed project root: {project_root}")
    sys.exit(1)

def run_local_write_test():
    print("--- DEBUG: Local Spark Write (No Azure) ---")
    print(f"Python Executable: {sys.executable}")

    # 1. Config (Minimal)
    config = {'env': 'dev'}
    
    # 2. Init Spark
    spark = create_spark_session(config)
    
    # 3. Create simple DataFrame
    print("\n>>> Creating test DataFrame...")
    data = [("A", 1), ("B", 2), ("C", 3)]
    df = spark.createDataFrame(data, ["letter", "number"])
    df.show()

    # 4. Define local path
    output_dir = os.path.join(project_root, "data", "debug_output")
    
    # Clean previous run
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    print(f"\n>>> Attempting LOCAL write to: {output_dir}")
    
    try:
        # Try writing PARQUET (uses PyArrow/Snappy - common crash points)
        df.write.mode("overwrite").parquet(output_dir)
        print("\n[SUCCESS] Local Parquet write succeeded!")
        print(f"Files created in: {output_dir}")
        
    except Exception as e:
        print(f"\n[FAILURE] Local write failed.")
        print(f"Error: {e}")

    spark.stop()

if __name__ == "__main__":
    run_local_write_test()
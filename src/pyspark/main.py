import sys
import os
import json

# --- MODULAR IMPORTS ---
try:
    from spark_setup import create_spark_session
    from gus_client import GusClient
    from utils import load_config, parse_arguments, ensure_container_exists
    from transformer import build_fact_table, create_metrics_dimension, create_calendar_dimension, create_regions_dimension
    import loader
except ImportError as e:
    print(f"CRITICAL ERROR: Missing modules. {e}")
    sys.exit(1)

def load_metrics_config(project_root):
    """Loads the list of metrics to process from configs/gus_metrics.json"""
    metrics_path = os.path.join(project_root, "configs", "gus_metrics.json")
    try:
        with open(metrics_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading metrics config: {e}")
        sys.exit(1)

def main():
    print(f"--- ETL Job Started (Star Schema Mode) ---")

    # 1. CONFIGURATION
    args = parse_arguments()
    config = load_config(args.config)
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    
    metrics = load_metrics_config(project_root)
    raw_data_dir = config['paths']['raw']

    # ---------------------------------------------------------
    # STEP 1: EXTRACT (Download)
    # ---------------------------------------------------------
    print("\n>>> STEP 1: EXTRACT (Downloading Raw Data)")
    
    # Check for API Key in config (optional)
    api_key = config.get('gus', {}).get('api_key')
    client = GusClient(api_key=api_key)
    
    for metric in metrics:
        if not metric.get('variable_id'):
            continue

        client.download_variable(
            variable_id=metric['variable_id'],
            variable_name=metric['name'],
            output_base_dir=raw_data_dir,
            unit_level=2, 
            force=args.force
        )

    # ---------------------------------------------------------
    # PRE-FLIGHT CHECK
    # ---------------------------------------------------------
    print("\n>>> PRE-FLIGHT CHECK")
    storage_conf = config.get('storage', {})
    conn_str = storage_conf.get('connection_string')
    container_name = storage_conf.get('container_name')
    ensure_container_exists(conn_str, container_name)

    # ---------------------------------------------------------
    # STEP 2: TRANSFORM (Building Star Schema)
    # ---------------------------------------------------------
    print("\n>>> STEP 2: TRANSFORM (Building Star Schema)")
    spark = create_spark_session(config)
    
    # A. Build the main Fact Table (Delegated to transformer)
    df_fact_combined = build_fact_table(spark, metrics, raw_data_dir)
    
    if not df_fact_combined:
        print("   [CRITICAL] No data found to process. Exiting.")
        spark.stop()
        sys.exit(1)

    # B. Create Dim_Region (Extract unique regions before dropping names)
    df_dim_region = create_regions_dimension(df_fact_combined)
    
    # C. Finalize Fact Table (Remove redundant text columns present in Dimensions)
    df_fact_final = df_fact_combined.drop("unit_name")

    # D. Create other Dimensions
    df_dim_metrics = create_metrics_dimension(spark, metrics)
    df_dim_calendar = create_calendar_dimension(spark)

    # ---------------------------------------------------------
    # STEP 3: LOAD (Upload to Azurite)
    # ---------------------------------------------------------
    print("\n>>> STEP 3: LOAD (Saving Parquet Files)")
    
    # 1. Fact_Economics
    loader.save_to_azurite(df_fact_final, "Fact_Economics", config)
    
    # 2. Dimensions
    loader.save_to_azurite(df_dim_region, "Dim_Region", config)
    loader.save_to_azurite(df_dim_metrics, "Dim_Metrics", config)
    loader.save_to_azurite(df_dim_calendar, "Dim_Calendar", config)

    spark.stop()
    print("\n--- ETL Job Finished Successfully ---")

if __name__ == "__main__":
    main()
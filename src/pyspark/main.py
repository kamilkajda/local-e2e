import sys
import os
import json
from spark_setup import create_spark_session
from gus_client import GusClient
from utils import load_config, parse_arguments, ensure_container_exists
import transformer
import loader

def main():
    print(f"--- ETL Job Started (Renamed Schema) ---")
    args = parse_arguments()
    config = load_config(args.config)
    
    # Path Resolution
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    raw_data_dir = os.path.abspath(config['paths']['raw'])
    
    # Load metrics mapping
    metrics_path = os.path.join(project_root, "configs", "gus_metrics.json")
    with open(metrics_path, 'r', encoding='utf-8') as f:
        metrics = json.load(f)

    # STEP 1: EXTRACT
    print("\n>>> STEP 1: EXTRACT")
    client = GusClient(api_key=config.get('gus', {}).get('api_key'))
    client.download_all_metrics(metrics, raw_data_dir, levels=[0, 2], force=args.force)

    # STEP 2 & 3: TRANSFORM & LOAD
    print("\n>>> STEP 2 & 3: TRANSFORM & LOAD")
    ensure_container_exists(config['storage']['connection_string'], config['storage']['container_name'])
    spark = create_spark_session(config)
    
    # Process Facts (returns internal DF with region_source for dimension building)
    df_internal = transformer.process_facts(spark, metrics, raw_data_dir)
    
    if df_internal:
        print("   [Main] Data processed. Cleaning facts and building dimensions...")
        
        # 1. Clean Fact Table: Remove 'region_source' before saving to storage
        df_facts_final = df_internal.drop("region_source")
        loader.save_to_azurite(df_facts_final, "fact_economics", config)
        
        # 2. Build Dimensions (using the internal DF that still has region_source)
        loader.save_to_azurite(transformer.create_regions_dimension(df_internal), "dim_region", config)
        loader.save_to_azurite(transformer.create_metrics_dimension(spark, metrics), "dim_metrics", config)
        loader.save_to_azurite(transformer.create_calendar_dimension(spark, df_internal), "dim_calendar", config)
    else:
        print("   [Main] WARNING: No facts were processed. Check raw data.")

    spark.stop()
    print("\n--- ETL Job Finished Successfully ---")

if __name__ == "__main__":
    main()
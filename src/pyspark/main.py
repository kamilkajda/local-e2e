import sys
import os
import json
from spark_setup import create_spark_session
from gus_client import GusClient
from utils import load_config, parse_arguments, ensure_container_exists
import transformer
import loader

def main():
    print(f"--- ETL Job Started ---")
    args = parse_arguments()
    config = load_config(args.config)
    
    # Path Resolution
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    raw_data_dir = os.path.abspath(config['paths']['raw'])
    with open(os.path.join(project_root, "configs", "gus_metrics.json"), 'r', encoding='utf-8') as f:
        metrics = json.load(f)

    # STEP 1: EXTRACT
    print("\n>>> STEP 1: EXTRACT")
    client = GusClient(api_key=config.get('gus', {}).get('api_key'))
    client.download_all_metrics(metrics, raw_data_dir, levels=[0, 2], force=args.force)

    # STEP 2 & 3: TRANSFORM & LOAD
    print("\n>>> STEP 2 & 3: TRANSFORM & LOAD")
    ensure_container_exists(config['storage']['connection_string'], config['storage']['container_name'])
    spark = create_spark_session(config)
    
    # Process Facts
    df_facts = transformer.process_facts(spark, metrics, raw_data_dir)
    if df_facts:
        loader.save_to_azurite(df_facts, "fact_economics", config)
        
        # Process Dimensions (Using Facts for consistency)
        loader.save_to_azurite(transformer.create_regions_dimension(df_facts), "dim_region", config)
        loader.save_to_azurite(transformer.create_metrics_dimension(spark, metrics), "dim_metrics", config)
        loader.save_to_azurite(transformer.create_calendar_dimension(spark), "dim_calendar", config)

    spark.stop()
    print("\n--- ETL Job Finished Successfully ---")

if __name__ == "__main__":
    main()
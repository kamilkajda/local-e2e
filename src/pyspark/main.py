import json
import os

import loader
import transformer
from gus_client import GusClient
from spark_setup import create_spark_session

from utils import ensure_container_exists, load_config, parse_arguments


def main():
    """
    Primary entry point for the E2E ETL pipeline.
    Manages the lifecycle of data extraction from GUS API, transformation via PySpark,
    and multi-dimensional loading into Azurite storage.
    """
    print("--- ETL Job Orchestration Started ---")

    # Initialization of runtime arguments and environment configuration
    args = parse_arguments()
    config = load_config(args.config)

    # Dynamic path resolution to ensure portability across different local environments
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    raw_data_dir = os.path.abspath(config["paths"]["raw"])

    # Load metric definitions for API targeting
    metrics_path = os.path.join(project_root, "configs", "gus_metrics.json")
    with open(metrics_path, "r", encoding="utf-8") as f:
        metrics = json.load(f)

    # DATA EXTRACTION PHASE
    # Communicates with GUS BDL API to persist raw JSON telemetry
    print("\n>>> Phase 1: Ingestion (API Extraction)")
    client = GusClient(api_key=config.get("gus", {}).get("api_key"))
    client.download_all_metrics(metrics, raw_data_dir, levels=[0, 2], force=args.force)

    # TRANSFORMATION & LOAD PHASE
    # Requires active SparkSession and verified Azurite container state
    print("\n>>> Phase 2: Processing & Persistence (Spark Transformation)")
    ensure_container_exists(
        config["storage"]["connection_string"], config["storage"]["container_name"]
    )
    spark = create_spark_session(config)

    # Executes business logic and global data imputation (interpolation)
    # Returns an internal DataFrame mapped with regional sources for dimension bridging
    df_internal = transformer.process_facts(spark, metrics, raw_data_dir)

    if df_internal:
        print("   [Main] Transformation complete. Generating relational assets...")

        # Fact Table: Removal of transient columns before cloud synchronization
        df_facts_final = df_internal.drop("region_source")
        loader.save_to_azurite(df_facts_final, "fact_economics", config)

        # Dimension Tables: Derived from the internal analytical dataset
        loader.save_to_azurite(
            transformer.create_regions_dimension(df_internal), "dim_region", config
        )
        loader.save_to_azurite(
            transformer.create_metrics_dimension(spark, metrics), "dim_metrics", config
        )
        loader.save_to_azurite(
            transformer.create_calendar_dimension(spark, df_internal),
            "dim_calendar",
            config,
        )
    else:
        print("   [Main] WARNING: Transformation resulted in an empty dataset. Pipeline aborted.")

    # Graceful shutdown of Spark resource manager
    spark.stop()
    print("\n--- ETL Job Orchestration Finished Successfully ---")


if __name__ == "__main__":
    main()

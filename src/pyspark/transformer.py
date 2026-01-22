from functools import reduce
import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
import datetime

def flatten_gus_data(df: DataFrame) -> DataFrame:
    """
    Flattens the nested GUS JSON structure into a tabular format.
    Structure: results[] -> values[] -> {year, val}
    """
    # 1. Explode top-level results
    df_units = df.select(F.explode(F.col("results")).alias("unit"))
    
    # 2. Explode values array
    df_flat = df_units.select(
        F.col("unit.id").alias("unit_id"),
        F.col("unit.name").alias("unit_name"),
        F.explode(F.col("unit.values")).alias("val_obj")
    )

    # 3. Select and cast columns
    final_df = df_flat.select(
        F.col("unit_id"),
        F.col("unit_name"),
        F.col("val_obj.year").cast(IntegerType()).alias("year"),
        F.col("val_obj.val").cast("double").alias("value")
    )
    
    return final_df

def build_fact_table(spark: SparkSession, metrics_list: list, raw_data_dir: str) -> DataFrame:
    """
    Reads raw JSON files for all metrics, flattens them, and merges into a single Fact Table.
    Handles file reading, error catching, and union logic.
    """
    print("   [Transformer] Building Fact Table from raw files...")
    fact_dataframes = []
    
    for metric in metrics_list:
        if not metric.get('variable_id'): continue
            
        metric_name = metric['name']
        variable_id = int(metric['variable_id'])
        # Construct path with wildcard to catch all parts
        input_path = os.path.join(raw_data_dir, metric_name, "*.json")
        
        try:
            # Spark handles wildcard paths nicely
            # multiLine=True is required for GUS standard JSON format
            df_raw = spark.read.option("multiLine", True).json(input_path)
            
            if df_raw.rdd.isEmpty():
                 print(f"      [WARNING] Skipping empty source: {metric_name}")
                 continue

            # Flatten structure
            df_flat = flatten_gus_data(df_raw)
            
            # Add metadata column (Foreign Key to Dim_Metrics)
            df_with_id = df_flat.withColumn("variable_id", F.lit(variable_id))
            
            fact_dataframes.append(df_with_id)
            print(f"      [Transform] Processed metric: {metric_name}")
            
        except Exception as e:
            # Usually happens if path doesn't exist (no data downloaded)
            # We catch it to allow other metrics to proceed
            print(f"      [INFO] Could not process {metric_name} (might be empty). Error: {str(e).splitlines()[0]}")

    if not fact_dataframes:
        return None

    print(f"   [Transformer] Merging {len(fact_dataframes)} dataframes into Fact_Economics...")
    # Union all collected dataframes into one vertical table
    return reduce(DataFrame.unionByName, fact_dataframes)

def create_regions_dimension(df_all_facts: DataFrame) -> DataFrame:
    """
    Extracts unique regions (unit_id, unit_name) from the combined fact table.
    Used to create Dim_Region.
    """
    print("   [Transformer] Extracting unique Regions Dimension...")
    return df_all_facts.select("unit_id", "unit_name").distinct().orderBy("unit_name")

def create_metrics_dimension(spark: SparkSession, metrics_list: list) -> DataFrame:
    """
    Creates a dimension DataFrame from the metrics configuration list.
    """
    print("   [Transformer] Creating Metrics Dimension...")
    
    schema = StructType([
        StructField("variable_id", IntegerType(), True),
        StructField("metric_name", StringType(), True),
        StructField("description", StringType(), True)
    ])
    
    data = []
    for m in metrics_list:
        if m.get('variable_id'):
            data.append((
                int(m['variable_id']), 
                m['name'], 
                m.get('description', '')
            ))
            
    return spark.createDataFrame(data, schema)

def create_calendar_dimension(spark: SparkSession) -> DataFrame:
    """
    Generates a Calendar Dimension DataFrame using Spark logic.
    Range: 2000 to Current Year + 2 (dynamic).
    """
    current_real_year = datetime.datetime.now().year
    start_year = 2000
    end_year = current_real_year + 2
    
    print(f"   [Transformer] Creating Calendar Dimension ({start_year}-{end_year})...")
    
    # Create a range of years
    df_years = spark.range(start_year, end_year + 1).toDF("year")
    
    # Add derived columns
    df_calendar = df_years.withColumn(
        "decade",
        (F.floor(F.col("year") / 10) * 10).cast(IntegerType())
    )
    
    return df_calendar
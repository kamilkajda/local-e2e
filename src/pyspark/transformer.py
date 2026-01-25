from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
import datetime
import os

def flatten_gus_data(df: DataFrame) -> DataFrame:
    df_units = df.select(F.explode(F.col("results")).alias("unit"))
    df_flat = df_units.select(
        F.col("unit.id").alias("unit_id"),
        F.col("unit.name").alias("unit_name"),
        F.explode(F.col("unit.values")).alias("val_obj")
    )
    return df_flat.select(
        F.col("unit_id"),
        F.col("unit_name"),
        F.col("val_obj.year").cast(IntegerType()).alias("year"),
        F.col("val_obj.val").cast("double").alias("value")
    )

def process_facts(spark: SparkSession, metrics: list, raw_data_dir: str):
    """
    High-level orchestrator for processing all fact metrics.
    Returns a unified fact DataFrame.
    """
    all_facts = []
    print("   [Transformer] Processing metrics into facts...")
    
    for metric in metrics:
        if not metric.get('variable_id'): continue
        name = metric['name']
        path = os.path.join(raw_data_dir, name, "*.json")
        
        try:
            df_raw = spark.read.option("multiLine", True).json(path)
            if df_raw.rdd.isEmpty(): continue
            
            df_flat = flatten_gus_data(df_raw)
            df_with_id = df_flat.withColumn("variable_id", F.lit(int(metric['variable_id'])))
            all_facts.append(df_with_id)
        except Exception:
            continue

    if not all_facts:
        return None
        
    # Union all dataframes into one big fact table
    from functools import reduce
    return reduce(DataFrame.unionByName, all_facts)

def create_regions_dimension(df_all_facts: DataFrame) -> DataFrame:
    print("   [Transformer] Creating Regions Dim...")
    df_regions = df_all_facts.select("unit_id", "unit_name").distinct()
    return df_regions.withColumn(
        "level_type",
        F.when((F.col("unit_id") == "000000000000") | (F.col("unit_id") == "0"), "Country")
        .otherwise("Voivodeship")
    ).orderBy("unit_name")

def create_metrics_dimension(spark: SparkSession, metrics_list: list) -> DataFrame:
    schema = StructType([
        StructField("variable_id", IntegerType(), True),
        StructField("metric_name", StringType(), True),
        StructField("description", StringType(), True)
    ])
    data = [(int(m['variable_id']), m['name'], m.get('description', '')) for m in metrics_list if m.get('variable_id')]
    return spark.createDataFrame(data, schema)

def create_calendar_dimension(spark: SparkSession) -> DataFrame:
    start_year, end_year = 2000, datetime.datetime.now().year + 2
    return spark.range(start_year, end_year + 1).toDF("year") \
                .withColumn("decade", (F.floor(F.col("year") / 10) * 10).cast(IntegerType()))
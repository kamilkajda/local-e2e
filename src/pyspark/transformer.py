from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
import datetime
import os

def flatten_gus_data(df: DataFrame) -> DataFrame:
    """
    Explodes the nested GUS JSON structure into a flat format using the new schema names.
    Note: region_source is kept here to allow building the regions dimension later.
    """
    df_units = df.select(F.explode(F.col("results")).alias("unit"))
    df_flat = df_units.select(
        F.col("unit.id").alias("region_id"),
        F.col("unit.name").alias("region_source"),
        F.explode(F.col("unit.values")).alias("val_obj")
    )
    return df_flat.select(
        F.col("region_id"),
        F.col("region_source"),
        F.col("val_obj.year").cast(IntegerType()).alias("year"),
        F.col("val_obj.val").cast("double").alias("value")
    )

def process_facts(spark: SparkSession, metrics: list, raw_data_dir: str):
    """
    Orchestrates processing of all fact metrics into a unified table with metric_id.
    """
    all_facts = []
    print("   [Transformer] Processing metrics into facts (Metric ID Schema)...")
    
    for metric in metrics:
        if not metric.get('variable_id'): continue
        name = metric['name']
        path = os.path.join(raw_data_dir, name, "*.json")
        
        try:
            df_raw = spark.read.option("multiLine", True).json(path)
            if df_raw.rdd.isEmpty(): continue
            
            df_flat = flatten_gus_data(df_raw)
            # Assign metric_id from variable_id in config
            df_with_id = df_flat.withColumn("metric_id", F.lit(int(metric['variable_id'])))
            all_facts.append(df_with_id)
        except Exception:
            continue

    if not all_facts:
        return None
        
    from functools import reduce
    return reduce(DataFrame.unionByName, all_facts)

def create_regions_dimension(df_all_facts: DataFrame) -> DataFrame:
    """
    Creates Regions Dimension. Renames 'POLSKA' to 'NATIONAL' in the primary 'region' column.
    """
    print("   [Transformer] Creating Regions Dim (region & region_id schema)...")
    
    df_regions = df_all_facts.select("region_id", "region_source").distinct()
    
    return df_regions.withColumn(
        "region", 
        F.when(F.upper(F.col("region_source")) == "POLSKA", "NATIONAL")
        .otherwise(F.col("region_source"))
    ).withColumn(
        "sort_order",
        F.when(F.col("region") == "NATIONAL", 0).otherwise(1)
    ).withColumn(
        "level_type",
        F.when((F.col("region_id") == "000000000000") | (F.col("region_id") == "0"), "Country")
        .otherwise("Voivodeship")
    ).orderBy("sort_order", "region")

def create_metrics_dimension(spark: SparkSession, metrics_list: list) -> DataFrame:
    """
    Converts metrics configuration into a formal dimension table with metric_id.
    """
    schema = StructType([
        StructField("metric_id", IntegerType(), True),
        StructField("metric_name", StringType(), True),
        StructField("description", StringType(), True)
    ])
    data = [(int(m['variable_id']), m['name'], m.get('description', '')) for m in metrics_list if m.get('variable_id')]
    return spark.createDataFrame(data, schema)

def create_calendar_dimension(spark: SparkSession, df_facts: DataFrame) -> DataFrame:
    """
    Creates a calendar table from 2000 up to the maximum year found in facts.
    """
    start_year = 2000
    max_year_row = df_facts.select(F.max("year")).collect()[0][0]
    end_year = max_year_row if max_year_row else datetime.datetime.now().year
    
    return spark.range(start_year, end_year + 1).toDF("year") \
                .withColumn("decade", (F.floor(F.col("year") / 10) * 10).cast(IntegerType()))
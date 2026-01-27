from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
import datetime
import os

def flatten_gus_data(df: DataFrame) -> DataFrame:
    """
    Explodes the nested GUS JSON structure. 
    Added 'attr_id' to distinguish real zeros from data gaps/confidentiality.
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
        F.col("val_obj.val").cast("double").alias("value"),
        F.col("val_obj.attrId").cast(IntegerType()).alias("attr_id")
    )

def impute_missing_values(df: DataFrame) -> DataFrame:
    """
    Performs linear interpolation for missing/confidential data (attr_id != 1).
    Fills 0.0 values with the average of the preceding and succeeding years.
    Drops internal helper columns (including attr_id) before returning.
    """
    # Define window: partition by region and metric, order by year
    window_spec = Window.partitionBy("region_id", "metric_id").orderBy("year")
    
    # Get neighbors
    df_neighbors = df.withColumn("prev_val", F.lag("value").over(window_spec)) \
                     .withColumn("next_val", F.lead("value").over(window_spec))
    
    # Logic: If value is 0.0 and NOT Final (attr_id != 1), use average of neighbors
    df_imputed = df_neighbors.withColumn("value", 
        F.when(
            ((F.col("value") == 0.0) | (F.col("value").isNull())) & (F.col("attr_id") != 1),
            (F.col("prev_val") + F.col("next_val")) / 2
        ).otherwise(F.col("value"))
    )
    
    # Clean up: remove internal columns used for calculation
    return df_imputed.drop("prev_val", "next_val", "attr_id")

def process_facts(spark: SparkSession, metrics: list, raw_data_dir: str):
    """
    Orchestrates processing. Includes global data imputation (interpolation).
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
            df_with_id = df_flat.withColumn("metric_id", F.lit(int(metric['variable_id'])))
            all_facts.append(df_with_id)
        except Exception:
            continue

    if not all_facts:
        return None
        
    from functools import reduce
    df_combined = reduce(DataFrame.unionByName, all_facts)
    
    # Apply global interpolation for gaps
    print("   [Transformer] Applying global data imputation (Interpolation for gaps)...")
    return impute_missing_values(df_combined)

def create_regions_dimension(df_all_facts: DataFrame) -> DataFrame:
    """
    Creates Regions Dimension. Renames 'POLSKA' to 'NATIONAL'.
    """
    print("   [Transformer] Creating Regions Dim...")
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
    Converts metrics configuration into a formal dimension table.
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
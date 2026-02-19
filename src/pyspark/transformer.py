import datetime
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.window import Window


def flatten_gus_data(df: DataFrame) -> DataFrame:
    """
    Normalizes the hierarchical GUS BDL JSON structure into a flattened relational schema.
    Extracts regional identifiers and time-series arrays while preserving the attribute
    metadata (attr_id) for downstream data quality validation.
    """
    # Isolate unit results and explode nested time-series values
    df_units = df.select(F.explode(F.col("results")).alias("unit"))
    df_flat = df_units.select(
        F.col("unit.id").alias("region_id"),
        F.col("unit.name").alias("region_source"),
        F.explode(F.col("unit.values")).alias("val_obj"),
    )

    # Cast to strictly typed columns for schema consistency
    return df_flat.select(
        F.col("region_id"),
        F.col("region_source"),
        F.col("val_obj.year").cast(IntegerType()).alias("year"),
        F.col("val_obj.val").cast("double").alias("value"),
        F.col("val_obj.attrId").cast(IntegerType()).alias("attr_id"),
    )


def impute_missing_values(df: DataFrame) -> DataFrame:
    """
    Implements data imputation using linear interpolation for records flagged as non-final
    or confidential (attr_id != 1). Replaces zero or null observations with the
    arithmetic mean of adjacent temporal neighbors within the same regional/metric partition.
    """
    # Partition by region and metric to ensure temporal continuity per series
    window_spec = Window.partitionBy("region_id", "metric_id").orderBy("year")

    # Capture boundary values for interpolation logic
    df_neighbors = df.withColumn("prev_val", F.lag("value").over(window_spec)).withColumn(
        "next_val", F.lead("value").over(window_spec)
    )

    # Apply imputation logic: prioritize neighbors for non-final/missing data points
    df_imputed = df_neighbors.withColumn(
        "value",
        F.when(
            ((F.col("value") == 0.0) | (F.col("value").isNull())) & (F.col("attr_id") != 1),
            (F.col("prev_val") + F.col("next_val")) / 2,
        ).otherwise(F.col("value")),
    )

    # Remove transient calculation columns and internal metadata before serving
    return df_imputed.drop("prev_val", "next_val", "attr_id")


def process_facts(spark: SparkSession, metrics: list, raw_data_dir: str):
    """
    Orchestrates the transformation pipeline by aggregating multiple JSON sources
    into a unified fact table. Executes global data quality rules including
    metric identification and cross-metric value imputation.
    """
    all_facts = []
    print("   [Transformer] Aggregating metric sources into fact schema...")

    for metric in metrics:
        if not metric.get("variable_id"):
            continue
        name = metric["name"]
        path = os.path.join(raw_data_dir, name, "*.json")

        try:
            # MultiLine enabled to handle paginated GUS API response structures
            df_raw = spark.read.option("multiLine", True).json(path)
            if df_raw.rdd.isEmpty():
                continue

            df_flat = flatten_gus_data(df_raw)
            df_with_id = df_flat.withColumn("metric_id", F.lit(int(metric["variable_id"])))
            all_facts.append(df_with_id)
        except Exception:
            continue

    if not all_facts:
        return None

    from functools import reduce

    df_combined = reduce(DataFrame.unionByName, all_facts)

    # Execute temporal gap filling for data consistency
    print("   [Transformer] Executing global interpolation for missing observations...")
    return impute_missing_values(df_combined)


def create_regions_dimension(df_all_facts: DataFrame) -> DataFrame:
    """
    Derives the geographic dimension table. Implements naming normalization
    for national benchmarks and establishes the TERYT-based hierarchy levels.
    """
    print("   [Transformer] Deriving Regions dimension...")
    df_regions = df_all_facts.select("region_id", "region_source").distinct()

    return (
        df_regions.withColumn(
            "region",
            F.when(F.upper(F.col("region_source")) == "POLSKA", "NATIONAL").otherwise(
                F.col("region_source")
            ),
        )
        .withColumn("sort_order", F.when(F.col("region") == "NATIONAL", 0).otherwise(1))
        .withColumn(
            "level_type",
            F.when(
                (F.col("region_id") == "000000000000") | (F.col("region_id") == "0"),
                "Country",
            ).otherwise("Voivodeship"),
        )
        .orderBy("sort_order", "region")
    )


def create_metrics_dimension(spark: SparkSession, metrics_list: list) -> DataFrame:
    """
    Converts metrics configuration metadata into a structured dimension table
    for front-end cataloging and filtering.
    """
    schema = StructType(
        [
            StructField("metric_id", IntegerType(), True),
            StructField("metric_name", StringType(), True),
            StructField("description", StringType(), True),
        ]
    )
    data = [
        (int(m["variable_id"]), m["name"], m.get("description", ""))
        for m in metrics_list
        if m.get("variable_id")
    ]
    return spark.createDataFrame(data, schema)


def create_calendar_dimension(spark: SparkSession, df_facts: DataFrame) -> DataFrame:
    """
    Generates a continuous time dimension. Dynamically calculates the range
    from the base year (2000) to the latest observed year in the fact table.
    """
    start_year = 2000
    max_year_row = df_facts.select(F.max("year")).collect()[0][0]
    end_year = max_year_row if max_year_row else datetime.datetime.now().year

    return (
        spark.range(start_year, end_year + 1)
        .toDF("year")
        .withColumn("decade", (F.floor(F.col("year") / 10) * 10).cast(IntegerType()))
    )

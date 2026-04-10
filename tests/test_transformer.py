import os
import sys

import pytest
from pyspark.sql import SparkSession

from src.pyspark.spark_setup import _configure_windows_environment
from src.pyspark.transformer import (
    create_calendar_dimension,
    create_metrics_dimension,
    create_regions_dimension,
    flatten_gus_data,
    impute_missing_values,
)


@pytest.fixture(scope="session")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    _configure_windows_environment()  # no-op on Linux/Mac, sets WinUtils on Windows
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_transformer")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# --- flatten_gus_data ---


def test_flatten_gus_data_schema(spark):
    from pyspark.sql import Row

    data = [
        Row(
            results=[
                Row(
                    id="PL001",
                    name="POLSKA",
                    values=[Row(year=2020, val=5000.0, attrId=1)],
                ),
                Row(
                    id="PL002",
                    name="Mazowieckie",
                    values=[
                        Row(year=2020, val=4000.0, attrId=1),
                        Row(year=2021, val=4200.0, attrId=1),
                    ],
                ),
            ]
        )
    ]
    df = spark.createDataFrame(data)
    result = flatten_gus_data(df)

    assert set(result.columns) == {"region_id", "region_source", "year", "value", "attr_id"}
    assert result.count() == 3


def test_flatten_gus_data_values(spark):
    from pyspark.sql import Row

    data = [
        Row(
            results=[
                Row(
                    id="PL001",
                    name="POLSKA",
                    values=[Row(year=2022, val=6000.0, attrId=1)],
                )
            ]
        )
    ]
    df = spark.createDataFrame(data)
    result = flatten_gus_data(df).collect()[0]

    assert result["region_id"] == "PL001"
    assert result["region_source"] == "POLSKA"
    assert result["year"] == 2022
    assert result["value"] == 6000.0
    assert result["attr_id"] == 1


# --- impute_missing_values ---


def test_impute_interpolates_zero_non_final(spark):
    data = [
        ("R1", 1, 2020, 1000.0, 1),
        ("R1", 1, 2021, 0.0, 2),  # non-final, zero → interpolate
        ("R1", 1, 2022, 2000.0, 1),
    ]
    df = spark.createDataFrame(data, ["region_id", "metric_id", "year", "value", "attr_id"])
    result = {r["year"]: r["value"] for r in impute_missing_values(df).collect()}

    assert result[2020] == 1000.0
    assert result[2021] == 1500.0  # (1000 + 2000) / 2
    assert result[2022] == 2000.0


def test_impute_keeps_final_zero(spark):
    data = [
        ("R1", 1, 2020, 1000.0, 1),
        ("R1", 1, 2021, 0.0, 1),  # attr_id=1 (final) → keep zero
        ("R1", 1, 2022, 2000.0, 1),
    ]
    df = spark.createDataFrame(data, ["region_id", "metric_id", "year", "value", "attr_id"])
    result = {r["year"]: r["value"] for r in impute_missing_values(df).collect()}

    assert result[2021] == 0.0


def test_impute_drops_attr_id_column(spark):
    data = [("R1", 1, 2020, 1000.0, 1)]
    df = spark.createDataFrame(data, ["region_id", "metric_id", "year", "value", "attr_id"])
    result = impute_missing_values(df)

    assert "attr_id" not in result.columns


# --- create_regions_dimension ---


def test_regions_normalizes_polska(spark):
    data = [("000000000000", "POLSKA"), ("020000000000", "Mazowieckie")]
    df = spark.createDataFrame(data, ["region_id", "region_source"])
    rows = {r["region_source"]: r for r in create_regions_dimension(df).collect()}

    assert rows["POLSKA"]["region"] == "NATIONAL"
    assert rows["POLSKA"]["sort_order"] == 0
    assert rows["POLSKA"]["level_type"] == "Country"


def test_regions_classifies_voivodeship(spark):
    data = [("020000000000", "Mazowieckie")]
    df = spark.createDataFrame(data, ["region_id", "region_source"])
    result = create_regions_dimension(df).collect()[0]

    assert result["region"] == "Mazowieckie"
    assert result["sort_order"] == 1
    assert result["level_type"] == "Voivodeship"


# --- create_metrics_dimension ---


def test_metrics_dimension_filters_missing_variable_id(spark):
    metrics = [
        {"variable_id": "123", "name": "GDP", "description": "Gross domestic product"},
        {"variable_id": None, "name": "SKIP"},
    ]
    result = create_metrics_dimension(spark, metrics).collect()

    assert len(result) == 1
    assert result[0]["metric_id"] == 123
    assert result[0]["metric_name"] == "GDP"
    assert result[0]["description"] == "Gross domestic product"


# --- create_calendar_dimension ---


def test_calendar_dimension_range(spark):
    data = [("R1", 1, 2023, 1000.0)]
    df = spark.createDataFrame(data, ["region_id", "metric_id", "year", "value"])
    rows = {r["year"]: r for r in create_calendar_dimension(spark, df).collect()}

    assert 2000 in rows
    assert 2023 in rows
    assert 2024 not in rows


def test_calendar_dimension_decade(spark):
    data = [("R1", 1, 2025, 1000.0)]
    df = spark.createDataFrame(data, ["region_id", "metric_id", "year", "value"])
    rows = {r["year"]: r for r in create_calendar_dimension(spark, df).collect()}

    assert rows[2020]["decade"] == 2020
    assert rows[2019]["decade"] == 2010
    assert rows[2000]["decade"] == 2000

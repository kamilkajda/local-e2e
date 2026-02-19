def add_inflation_adjustment(df: DataFrame, inflation_rate: float) -> DataFrame:
    """
    Adds a 'What-If' parameter for inflation adjustments in the Power BI report.

    Parameters:
    df (DataFrame): Fact table with metrics and values.
    inflation_rate (float): Rate of inflation adjustment. Defaults to 1.0 if not provided.

    Returns:
    DataFrame: Fact table with adjusted values.
    """
    # Apply inflation adjustment to fact table
    return df.withColumn(
        "adjusted_value",
        F.when(F.col("value") != 0, F.col("value") * (1 + inflation_rate)).otherwise(0),
    )

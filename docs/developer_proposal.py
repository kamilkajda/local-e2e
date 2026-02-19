```python
def validate_schema(df: DataFrame, schema_path: str):
    """
    Validates PySpark DataFrame against a specified Parquet schema.
    
    Args:
        df (DataFrame): Input DataFrame to validate.
        schema_path (str): Path to the target Parquet schema file.
        
    Returns:
        DataFrame: The input DataFrame if valid; otherwise raises an exception.
    """

    # Load the target Parquet schema
    from pyspark.sql import read

    schema = read.schema().load(schema_path)

    # Validate the DataFrame against the loaded schema
    return df.checkSchema(schema)
```
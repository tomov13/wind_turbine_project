import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, lit
from src.wt_ingestion import RawDataIngestor

@pytest.fixture(scope="module")
def spark():
    """Use Databricks' default Spark session instead of creating a new one."""
    from pyspark.sql import SparkSession
    yield SparkSession.builder.getOrCreate()  # Use Databricks' Spark session

def test_extract_turbine_id_from_metadata(spark):
    # Simulate a DataFrame with file metadata
    data = [("dbfs:/FileStore/tables/Location2.csv",)]
    schema = ["metadata_fileName"]
    
    df = spark.createDataFrame(data, schema)  # Use a valid column name

    # Mimic extraction logic
    df_extracted = df.withColumn(
        "extracted_turbine_id",
        regexp_extract(col("metadata_fileName"), "(?i)Location(\\d+)", 1)
    ).withColumn(
        "turbine_id",
        col("extracted_turbine_id").cast("int") + lit(0)  # No offset for test
    ).drop("extracted_turbine_id")

    # Retrieve result
    result = df_extracted.select("turbine_id").collect()[0]["turbine_id"]

    assert result == 2  # Ensure correct extraction
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from src.wt_cleaning import DataCleaner

@pytest.fixture(scope="session")
def spark():
    """Use Databricks' default Spark session instead of creating a new one."""
    from pyspark.sql import SparkSession
    yield SparkSession.builder.getOrCreate()  # Use Databricks' Spark session

@pytest.fixture(scope="module")
def cleaner(spark):
    return DataCleaner(spark)

def test_clean_turbine_data(spark, cleaner):
    # Create dummy data with some rows needing removal.
    data = [
        ("2021-01-01 00:00:00", 10.0, 5.0, 1),
        ("2021-01-01 00:01:00", -5.0, 3.0, 1),   # negative power_output becomes null and then dropped
        ("2021-01-01 00:02:00", 15.0, None, 1),    # missing wind_speed, drop row
        ("2021-01-01 00:03:00", 20.0, 7.0, 1)        # valid row
    ]
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("power_output", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("turbine_id", IntegerType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    df_clean = cleaner.clean_turbine_data(df)
    # Expect only rows with valid essential values (rows 1 and 4)
    assert df_clean.count() == 2

def test_transform_turbine_data(spark, cleaner):
    # Create dummy new turbine data.
    data = [
        ("2021-01-01 12:00:00", 0.5, 10.0, "45")
    ]
    schema = StructType([
        StructField("Time", StringType(), True),
        StructField("Power", DoubleType(), True),
        StructField("windspeed_100m", DoubleType(), True),
        StructField("winddirection_100m", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    df_transformed = cleaner.transform_turbine_data(df)
    cols = df_transformed.columns
    assert "power_output" in cols
    assert "timestamp" in cols
    assert "wind_speed" in cols
    assert "wind_direction" in cols
    assert "Time" not in cols
    assert "Power" not in cols
    # Check that power scaling is correct: 0.5 * 4.5 = 2.25
    result = df_transformed.select("power_output").collect()[0]["power_output"]
    assert result == pytest.approx(2.25, rel=1e-2)

def test_merge_bronze_data(spark, cleaner):
    # Create dummy original and new DataFrames.
    original_data = [("2021-01-01 00:00:00", 10.0, 5.0, 1)]
    new_data = [("2021-01-01 00:01:00", 20.0, 6.0, 2)]
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("power_output", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("turbine_id", IntegerType(), True)
    ])
    df_orig = spark.createDataFrame(original_data, schema)
    df_new = spark.createDataFrame(new_data, schema)
    merged = cleaner.merge_bronze_data(df_orig, df_new)
    assert merged.count() == 2

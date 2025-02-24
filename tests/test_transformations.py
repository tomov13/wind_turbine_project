import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from src.wt_transformations import DataTransformer

@pytest.fixture(scope="session")
def spark():
    """Use Databricks' default Spark session instead of creating a new one."""
    from pyspark.sql import SparkSession
    yield SparkSession.builder.getOrCreate()  # Use Databricks' Spark session

@pytest.fixture(scope="module")
def transformer(spark):
    return DataTransformer(spark)

def test_compute_expected_power(spark, transformer):
    data = [("2021-01-01 00:00:00", 10.0, 5.0, 1)]
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("power_output", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("turbine_id", IntegerType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    df = transformer.compute_expected_power(df)
    expected_power = 0.5 * 1.2 * 5024 * (5 ** 3) * 0.45 / 1e6
    result = df.select("expected_power").collect()[0]["expected_power"]
    assert result == pytest.approx(expected_power, rel=1e-4)

def test_detect_zscore_anomalies(spark, transformer):
    data = [
        ("2021-01-01 00:00:00", 10.0, 5.0, 1),
        ("2021-01-01 00:01:00", 15.0, 5.0, 1),
        ("2021-01-01 00:02:00", 20.0, 5.0, 1)
    ]
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("power_output", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("turbine_id", IntegerType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    df = transformer.compute_expected_power(df)
    df = transformer.detect_zscore_anomalies(df)
    assert "z_anomaly" in df.columns
    anomalies = [row["z_anomaly"] for row in df.select("z_anomaly").collect()]
    for a in anomalies:
        assert a in [0, 1]

def test_detect_record_anomalies(spark, transformer):
    data = [
        ("2021-01-01 00:00:00", 10.0, 5.0, 1),
        ("2021-01-01 00:01:00", 15.0, 5.0, 1),
        ("2021-01-01 00:02:00", 20.0, 5.0, 1)
    ]
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("power_output", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("turbine_id", IntegerType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    df = transformer.compute_expected_power(df)
    df = transformer.detect_record_anomalies(df)
    assert "if_anomaly" in df.columns
    anomalies = [row["if_anomaly"] for row in df.select("if_anomaly").collect()]
    for a in anomalies:
        assert a in [0, 1]

def test_combine_anomalies(spark, transformer):
    data = [
        (1, 0, 0),
        (1, 1, 0),
        (1, 0, 1),
        (1, 1, 1)
    ]
    schema = StructType([
        StructField("turbine_id", IntegerType(), True),
        StructField("z_anomaly", IntegerType(), True),
        StructField("if_anomaly", IntegerType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    df = transformer.combine_anomalies(df)
    combined = [row["combined_anomaly"] for row in df.select("combined_anomaly").collect()]
    assert combined == [0, 1, 1, 1]

def test_detect_turbine_anomalies(spark, transformer):
    data = [
        ("2021-01-01 00:00:00", 10.0, 5.0, 1, 1),
        ("2021-01-01 00:01:00", 10.0, 5.0, 1, 1),
        ("2021-01-01 00:00:00", 20.0, 6.0, 2, 0),
        ("2021-01-01 00:01:00", 20.0, 6.0, 2, 0)
    ]
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("power_output", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("turbine_id", IntegerType(), True),
        StructField("combined_anomaly", IntegerType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    turbine_anomalies = transformer.detect_turbine_anomalies(df)
    results = {row["turbine_id"]: row["turbine_status"] for row in turbine_anomalies.collect()}
    assert results[1] == "FAULTY_SENSOR"
    assert results[2] == "NORMAL"

def test_apply_smart_filtering(spark, transformer):
    data = [
        (1, "FAULTY_SENSOR", 10.0),
        (1, "NORMAL", 15.0),
        (2, "REVIEW_REQUIRED", 20.0),
        (2, "NORMAL", 25.0)
    ]
    schema = StructType([
        StructField("turbine_id", IntegerType(), True),
        StructField("turbine_status", StringType(), True),
        StructField("power_output", DoubleType(), True)
    ])
    df = spark.createDataFrame(data, schema)

    # Fix: Ensure combined_anomaly exists before calling detect_turbine_anomalies()
    df = df.withColumn("combined_anomaly", lit(0))  # Default to 0 for test purposes

    df_anomalies = transformer.detect_turbine_anomalies(df)

    # Apply smart filtering (renaming status to prevent ambiguity)
    filtered = transformer.apply_smart_filtering(
        df.withColumnRenamed("turbine_status", "status_copy"), 
        df_anomalies
    )

    assert filtered.count() > 0  # Ensure filtering didn't remove everything

def test_calculate_summary_statistics(spark, transformer):
    data = [
        ("2021-01-01 00:00:00", 10.0, 1),
        ("2021-01-01 00:30:00", 20.0, 1),
        ("2021-01-01 01:00:00", 30.0, 1)
    ]
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("power_output", DoubleType(), True),
        StructField("turbine_id", IntegerType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    summary = transformer.calculate_summary_statistics(df)
    # Expect one summary row for turbine_id 1.
    assert summary.count() == 1

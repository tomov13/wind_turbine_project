from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_extract, lit, col
from src.wt_logger import LoggerUtility

class RawDataIngestor:
    def __init__(self, spark, config=None):
        self.spark = spark
        self.logger = LoggerUtility.setup_logging()
        self.config = config or {}
        self.logger.info("RawDataIngestor initialized.")

    def load_original_turbine_data(self, directory_path: str) -> DataFrame:
        """
        Loads the original dataset from CSV with schema inference.
        Assumes the CSV already has a 'turbine_id' column.
        """
        try:
            self.logger.info(f"Loading original turbine data from {directory_path} (with inferSchema)")
            df = (self.spark.read
                  .format("csv")
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .option("cloudFiles.inferColumnTypes", "true")  # Enable file metadata
                  .load(f"{directory_path}/data_group_*.csv")
                  .cache())

            record_count = df.count()
            self.logger.info(f"Successfully loaded {record_count} original turbine dataset records.")
            self.logger.info("Using 'turbine_id' from the CSV itself.")
            return df
        except Exception as e:
            self.logger.error(f"Error loading original turbine data: {e}")
            raise

    def load_new_turbine_data(self, directory_path: str) -> DataFrame:
        """
        Loads new turbine data from CSV with schema inference.
        Uses max_existing_id to assign a unique turbine_id.
        Assumes filenames follow a pattern like 'Location<number>.csv' (case-insensitive).
        """
        try:
            self.logger.info(f"Loading new turbine data from {directory_path} (with inferSchema)")
            df = (self.spark.read
                  .format("csv")
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .option("cloudFiles.inferColumnTypes", "true")  # Ensure metadata is captured
                  .load(f"{directory_path}/Location*.csv")
                  .withColumn("_metadata.file_path", col("_metadata.file_path"))  # Capture file path
                  .cache())

            record_count = df.count()
            self.logger.info(f"Successfully loaded {record_count} new turbine dataset records.")

            # Get the maximum turbine_id from the original dataset.
            df_original_turbine_data = self.spark.read.table("bronze_data.original_turbine_bronze")
            max_existing_id = df_original_turbine_data.agg({"turbine_id": "max"}).collect()[0][0]
            if max_existing_id is None:
                max_existing_id = 0

            self.logger.info(f"Using maximum existing turbine_id: {max_existing_id}")

            # Extract turbine number from file metadata column
            df = df.withColumn(
                "extracted_turbine_id",
                regexp_extract(col("_metadata.file_path"), "Location(\\d+)", 1)
            )

            # Convert to integer, add the offset, and overwrite turbine_id column.
            df = df.withColumn(
                "turbine_id",
                col("extracted_turbine_id").cast("int") + lit(max_existing_id)
            ).drop("extracted_turbine_id")

            self.logger.info("Successfully added turbine_id to new turbine dataset records.")
            return df
        except Exception as e:
            self.logger.error(f"Error loading new turbine data: {e}")
            raise

    def write_bronze(self, df: DataFrame, table_name: str):
        try:
            self.logger.info(f"Writing data to DBFS Delta table: {table_name}")

            # Define DBFS storage path dynamically per user
            storage_path = f"dbfs:/FileStore/{table_name}/"

            # Ensure we are using the default Databricks Metastore (not Unity Catalog)
            self.spark.sql("USE CATALOG hive_metastore")

            # Ensure the database exists
            self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze_data")

            full_table_name = f"bronze_data.{table_name}"

            # Write DataFrame as a Delta Table (raw files stored in DBFS)
            df.write.mode("overwrite").format("delta").option("path", storage_path).saveAsTable(full_table_name)

            # Explicitly register the table in Databricks Metastore
            self.spark.sql(f"CREATE TABLE IF NOT EXISTS {full_table_name} USING DELTA LOCATION '{storage_path}'")

            self.logger.info(f"Successfully written data to {full_table_name} at {storage_path}")
        except Exception as e:
            self.logger.error(f"Error writing bronze data: {e}")
            raise

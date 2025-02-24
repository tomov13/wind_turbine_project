from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, when
from src.wt_logger import LoggerUtility

class DataCleaner:
    def __init__(self, spark, config=None):
        self.spark = spark
        self.logger = LoggerUtility.setup_logging()
        self.config = config or {}
        self.logger.info("DataCleaner initialized.")

    def clean_turbine_data(self, df: DataFrame) -> DataFrame:
        """
        Cleans the turbine dataset by:
          - Replacing negative power_output values with nulls.
          - Dropping rows with null values in essential columns (timestamp, power_output, wind_speed).
        """
        try:
            self.logger.info("Cleaning turbine data...")
            initial_count = df.count()
            # Replace negative power outputs with nulls.
            df = df.withColumn("power_output", when(col("power_output") < 0, None).otherwise(col("power_output")))
            # Drop rows with missing values in essential columns only.
            df_clean = df.dropna(subset=["timestamp", "power_output", "wind_speed"])
            final_count = df_clean.count()
            self.logger.info(f"Cleaned dataset: {initial_count} records reduced to {final_count} records after cleaning.")
            return df_clean
        except Exception as e:
            self.logger.error(f"Error cleaning turbine data: {e}")
            raise

    def transform_turbine_data(self, df: DataFrame) -> DataFrame:
        """
        Transforms the new turbine dataset by:
          - Scaling power from normalized (0-1) to MW (0-4.5)
          - Renaming columns to match the original dataset
          - Dropping unnecessary columns.
        """
        try:
            self.logger.info("Transforming turbine data before merging.")
            df_transformed = (df
                              .withColumn("power_output", col("Power") * 4.5)
                              .withColumn("timestamp", to_timestamp(col("Time"), "yyyy-MM-dd HH:mm:ss"))
                              .withColumnRenamed("windspeed_100m", "wind_speed")
                              .withColumnRenamed("winddirection_100m", "wind_direction")
                              .drop("Time", "temperature_2m", "relativehumidity_2m", "dewpoint_2m",
                                    "winddirection_10m", "windspeed_10m", "windgusts_10m", "Power", "_metadata.file_path")
                              .cache())
            self.logger.info("Turbine data transformation complete.")
            return df_transformed
        except Exception as e:
            self.logger.error(f"Error transforming turbine data: {e}")
            raise

    def merge_bronze_data(self, df_original: DataFrame, df_new: DataFrame) -> DataFrame:
        """
        Merges the original and new turbine datasets into a unified Silver dataset.
        Applies cleaning after merging.
        """
        try:
            original_count = df_original.count()
            new_count = df_new.count()
            self.logger.info(f"Merging {original_count} original records with {new_count} new records.")
            merged_df = df_original.unionByName(df_new, allowMissingColumns=False).cache()
            merged_count = merged_df.count()
            self.logger.info(f"Total records after merging: {merged_count}")
            merged_clean_df = self.clean_turbine_data(merged_df)
            return merged_clean_df
        except Exception as e:
            self.logger.error(f"Error merging turbine data: {e}")
            raise

    def save_silver_table(self, df: DataFrame, table_name: str):
        """Saves the cleaned dataset to a Silver Delta table."""
        try:
            self.logger.info(f"Saving cleaned data to silver_data.{table_name}")
            self.spark.sql("CREATE SCHEMA IF NOT EXISTS hive_metastore.silver_data")
            df.write.mode("overwrite").format("delta").saveAsTable(f"hive_metastore.silver_data.{table_name}")
            self.logger.info(f"Successfully saved cleaned data to silver_data.{table_name}")
        except Exception as e:
            self.logger.error(f"Error saving silver data: {e}")
            raise

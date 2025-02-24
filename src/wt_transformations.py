from pyspark.sql import DataFrame
from pyspark.sql.functions import (col, lit, pow, round, mean, stddev, abs as spark_abs,
                                   monotonically_increasing_id, count, sum as spark_sum,
                                   min as spark_min, max as spark_max, avg as spark_avg, window, when)
from sklearn.ensemble import IsolationForest
from src.wt_logger import LoggerUtility

class DataTransformer:
    def __init__(self, spark, config=None):
        """
        Initializes DataTransformer with configurable parameters.
          - zscore_threshold (default: 2.0)
          - contamination_rate for Isolation Forest (default: 0.15)
        """
        self.spark = spark
        self.logger = LoggerUtility.setup_logging()
        self.config = config or {}
        self.zscore_threshold = self.config.get("zscore_threshold", 2.0)
        self.contamination_rate = self.config.get("contamination_rate", 0.15)
        self.logger.info("DataTransformer initialized.")

    def compute_expected_power(self, df: DataFrame) -> DataFrame:
        """
        Computes expected power output using a simplified wind power equation:
          P = 0.5 * ρ * A * V³ * Cp
        (with ρ=1.2, A=5024, Cp=0.45), converting from watts to MW.
        """
        try:
            self.logger.info("Computing expected power output...")
            df = df.withColumn(
                "expected_power",
                0.5 * lit(1.2) *
                lit(5024) *
                pow(col("wind_speed"), 3) *
                lit(0.45) / lit(1_000_000)
            )
            self.logger.info("Expected power computation complete.")
            return df
        except Exception as e:
            self.logger.error(f"Error computing expected power: {e}")
            raise

    def detect_zscore_anomalies(self, df: DataFrame) -> DataFrame:
        """
        Detects anomalies using a z‑score method.
        Flags records as "z_anomaly" if the error (power_output - expected_power)
        deviates beyond zscore_threshold * standard deviation.
        """
        try:
            self.logger.info("Detecting anomalies using z‑score method...")
            df = df.withColumn("error", col("power_output") - col("expected_power"))
            stats = (df.select(
                        mean(col("error")).alias("mean_error"),
                        stddev(col("error")).alias("std_error")
                     ).collect()[0])
            mean_err = stats["mean_error"]
            std_err = stats["std_error"]
            self.logger.info(f"Mean error: {mean_err}, Std error: {std_err}")
            df = df.withColumn(
                "z_anomaly",
                when(
                    spark_abs(col("error") - lit(mean_err)) > self.zscore_threshold * lit(std_err),
                    lit(1)
                ).otherwise(lit(0))
            )
            self.logger.info("Z‑score anomaly detection complete.")
            return df
        except Exception as e:
            self.logger.error(f"Error in z‑score anomaly detection: {e}")
            raise

    def detect_record_anomalies(self, df: DataFrame) -> DataFrame:
        """
        Detects record-level anomalies using Isolation Forest on features:
        wind_speed, expected_power, and power_output.
        Returns a new column "if_anomaly" (1 for anomaly, 0 otherwise).
        """
        try:
            self.logger.info("Detecting record-level anomalies with Isolation Forest...")
            df = df.withColumn("record_id", monotonically_increasing_id())
            pdf = df.select("record_id", "wind_speed", "expected_power", "power_output").toPandas()
            iso_forest = IsolationForest(
                contamination=self.contamination_rate,
                n_estimators=200,
                max_samples='auto',
                random_state=42
            )
            pdf["iso_score"] = iso_forest.fit_predict(pdf[["wind_speed", "expected_power", "power_output"]])
            pdf["iso_score"] = pdf["iso_score"].apply(lambda x: 1 if x == -1 else 0)
            anomaly_pdf = pdf[["record_id", "iso_score"]]
            anomaly_sdf = self.spark.createDataFrame(anomaly_pdf)
            df = df.join(anomaly_sdf, on="record_id", how="left")
            df = (df
                  .withColumnRenamed("iso_score", "if_anomaly")
                  .withColumn("power_output", round("power_output", 2))
                  .withColumn("expected_power", round(col("expected_power"), 2))
                  .withColumn("error", round(col("power_output") - col("expected_power"), 2))
                  .drop("record_id")
                  )
            self.logger.info("Isolation Forest anomaly detection complete.")
            return df
        except Exception as e:
            self.logger.error(f"Error in record anomaly detection: {e}")
            raise

    def combine_anomalies(self, df: DataFrame) -> DataFrame:
        """
        Combines z‑score and Isolation Forest anomaly flags using OR logic,
        creating a new column "combined_anomaly".
        """
        try:
            self.logger.info("Combining anomalies using OR logic.")
            df = df.withColumn(
                "combined_anomaly",
                when((col("z_anomaly") == 1) | (col("if_anomaly") == 1), lit(1)).otherwise(lit(0))
            )
            return df
        except Exception as e:
            self.logger.error(f"Error combining anomalies: {e}")
            raise

    def detect_turbine_anomalies(self, df: DataFrame) -> DataFrame:
        """
        Identifies turbines with a high anomaly rate.
        Classifies turbine status as:
          - > 60% anomalies: FAULTY_SENSOR
          - 30-60% anomalies: REVIEW_REQUIRED
          - < 30% anomalies: NORMAL
        """
        try:
            self.logger.info("Detecting turbine anomalies...")
            turbine_anomaly_df = (df.groupBy("turbine_id")
                                  .agg(
                                      spark_sum("combined_anomaly").alias("total_anomalies"),
                                      count("*").alias("total_records")
                                  )
                                  .withColumn("anomaly_rate", col("total_anomalies") / col("total_records")))
            turbine_anomaly_df = turbine_anomaly_df.withColumn(
                "turbine_status",
                when(col("anomaly_rate") > 0.6, "FAULTY_SENSOR")
                .when(col("anomaly_rate") > 0.3, "REVIEW_REQUIRED")
                .otherwise("NORMAL")
            )
            self.logger.info("Turbine anomaly detection complete.")
            return turbine_anomaly_df
        except Exception as e:
            self.logger.error(f"Error detecting turbine anomalies: {e}")
            raise

    def apply_smart_filtering(self, df: DataFrame, turbine_anomaly_df: DataFrame) -> DataFrame:
        """
        Applies filtering based on turbine status:
          - Removes FAULTY_SENSOR turbines
          - Leaves REVIEW_REQUIRED (flagged) and NORMAL turbines.
        """
        try:
            self.logger.info("Applying smart filtering based on turbine status.")
            df = df.join(turbine_anomaly_df.select("turbine_id", "turbine_status"),
                         on="turbine_id", how="left")
            df_filtered = df.filter(col("turbine_status") != "FAULTY_SENSOR")
            self.logger.info("Smart filtering complete.")
            return df_filtered
        except Exception as e:
            self.logger.error(f"Error in smart filtering: {e}")
            raise

    def calculate_summary_statistics(self, df: DataFrame) -> DataFrame:
        """
        Computes summary statistics (min, max, avg power output) per turbine over a 24‑hour window.
        Returns turbine_id, window_start, min_power, max_power, and avg_power.
        """
        try:
            self.logger.info("Calculating summary statistics over a 24‑hour period...")
            summary_df = (df.groupBy("turbine_id", window(col("timestamp"), "24 hours"))
                          .agg(
                              round(spark_min("power_output"), 2).alias("min_power"),
                              round(spark_max("power_output"), 2).alias("max_power"),
                              round(spark_avg("power_output"), 2).alias("avg_power")
                          )
                          .select(
                              col("turbine_id"),
                              col("window.start").alias("window_start"),
                              col("min_power"),
                              col("max_power"),
                              col("avg_power")
                          ))
            self.logger.info("Summary statistics calculation complete.")
            return summary_df
        except Exception as e:
            self.logger.error(f"Error calculating summary statistics: {e}")
            raise

    def save_turbine_analysis(self, df: DataFrame, table_name: str = "gold_turbine_analysis", mode: str = "overwrite"):
        """
        Saves the final turbine dataset to a Gold Delta table.
        The write mode (overwrite/append) is configurable.
        """
        try:
            self.logger.info(f"Saving results to gold_data.{table_name} with mode {mode}")
            self.spark.sql("CREATE SCHEMA IF NOT EXISTS hive_metastore.gold_data")
            df.write.mode(mode).format("delta").saveAsTable(f"hive_metastore.gold_data.{table_name}")
            self.logger.info(f"Successfully saved to gold_data.{table_name}")
        except Exception as e:
            self.logger.error(f"Error saving turbine analysis: {e}")
            raise

    def save_summary_table(self, df: DataFrame, table_name: str = "gold_turbine_summary", mode: str = "overwrite"):
        """
        Saves summary statistics to a Gold Delta table.
        """
        try:
            self.logger.info(f"Saving summary statistics to gold_data.{table_name} with mode {mode}")
            self.spark.sql("CREATE SCHEMA IF NOT EXISTS hive_metastore.gold_data")
            df.write.mode(mode).format("delta").saveAsTable(f"hive_metastore.gold_data.{table_name}")
            self.logger.info(f"Successfully saved summary statistics to gold_data.{table_name}")
        except Exception as e:
            self.logger.error(f"Error saving summary table: {e}")
            raise

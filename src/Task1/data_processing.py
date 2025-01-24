from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType
import os
from typing import Tuple, Optional, List, Dict
import logging
from datetime import datetime



class DataProcessor:
    def __init__(self, spark: SparkSession, logger: Optional[logging.Logger] = None):
        self.spark = spark
        self.logger = logger or logging.getLogger(__name__)

    def process_files_for_user_agent(self, input_path: str, output_path: str,
                                     user_agent_filter: str = "some user agent") -> bool:
        """Main processing function that handles both impressions and clicks.

        Args:
            input_path: Path to input files
            output_path: Path for output files
            user_agent_filter: User agent to filter by, defaults to "some user agent"

        Returns:
            bool: True if processing was successful, False otherwise
        """
        try:
            # Log processing start and configuration
            self.logger.info("Starting file processing...")
            self.logger.info(f"Input path: {input_path}")
            self.logger.info(f"Output path: {output_path}")
            self.logger.info(f"User agent filter: {user_agent_filter}")

            # Verify paths exist
            if not os.path.exists(input_path):
                self.logger.error(f"Input path does not exist: {input_path}")
                return False

            # Create output directory if it doesn't exist
            os.makedirs(output_path, exist_ok=True)

            # Get all parquet files
            all_files = []
            try:
                all_files = [f for f in os.listdir(
                    input_path) if f.endswith('.parquet')]
            except Exception as e:
                self.logger.error(
                    f"Error listing files in input directory: {str(e)}")
                return False

            if not all_files:
                self.logger.warning(
                    "No parquet files found in input directory")
                return False

            # Group files by date and type
            impressions_by_date: Dict[str, List[str]] = {}
            clicks_by_date: Dict[str, List[str]] = {}

            for file in all_files:
                try:
                    date = self._extract_date_from_filename(file)
                    if 'impressions_processed_dk_' in file:
                        impressions_by_date.setdefault(date, []).append(file)
                    elif 'clicks_processed_dk_' in file:
                        clicks_by_date.setdefault(date, []).append(file)
                except Exception as e:
                    self.logger.error(
                        f"Error processing filename {file}: {str(e)}")
                    continue

            # Log summary of files found
            self.logger.info(f"Found {len(all_files)} total parquet files")
            self.logger.info(
                f"Impressions files by date: {', '.join(impressions_by_date.keys())}")
            self.logger.info(
                f"Clicks files by date: {', '.join(clicks_by_date.keys())}")

            # Process each date
            all_dates = sorted(
                set(list(impressions_by_date.keys()) + list(clicks_by_date.keys())))
            processed_dates = 0

            for date in all_dates:
                try:
                    success = self._process_date_files(input_path, output_path,
                                                       impressions_by_date.get(
                                                           date, []),
                                                       clicks_by_date.get(
                                                           date, []),
                                                       date, user_agent_filter)
                    if success:
                        processed_dates += 1
                        self.logger.info(
                            f"Successfully processed data for date: {date}")
                    else:
                        self.logger.warning(
                            f"Failed to process data for date: {date}")
                except Exception as e:
                    self.logger.warning(
                        f"Failed to process data for date: {date} - {str(e)}")
                    continue

            # Final success check
            if processed_dates > 0:
                self.logger.info(
                    f"Successfully processed {processed_dates} dates")
                return True
            else:
                self.logger.error("No dates were successfully processed")
                return False

        except Exception as e:
            self.logger.error(f"Critical error during processing: {str(e)}")
            import traceback
            self.logger.error(f"Stack trace: {traceback.format_exc()}")
            return False

    def _process_date_files(self, input_path: str, output_path: str,
                            impression_files: List[str], click_files: List[str],
                            date: str, user_agent_filter: str) -> bool:
        """Process impression and click files for a specific date."""
        try:
            # Process impressions
            impressions_df = None
            if impression_files:
                impression_paths = [os.path.join(
                    input_path, f) for f in impression_files]
                impressions_df = self.spark.read.parquet(*impression_paths)

                # Log distinct user agents before filtering
                distinct_agents = impressions_df.select(
                    'device_settings.user_agent').distinct().collect()
                self.logger.info(
                    f"Distinct user agents in impressions: {distinct_agents}")

                # Apply the correct filter
                impressions_df = impressions_df.filter(
                    F.col("device_settings.user_agent") == user_agent_filter
                )

                # Log count after filtering
                filtered_count = impressions_df.count()
                self.logger.info(
                    f"Found {filtered_count} impressions for user agent: {user_agent_filter}")

                impressions_df = self._process_file_type(impressions_df, date)

            # Process clicks similarly
            clicks_df = None
            if click_files:
                click_paths = [os.path.join(input_path, f)
                               for f in click_files]
                clicks_df = self.spark.read.parquet(*click_paths)

                # Log distinct user agents before filtering
                distinct_agents = clicks_df.select(
                    'device_settings.user_agent').distinct().collect()
                self.logger.info(
                    f"Distinct user agents in clicks: {distinct_agents}")

                # Apply the correct filter
                clicks_df = clicks_df.filter(
                    F.col("device_settings.user_agent") == user_agent_filter
                )

                filtered_count = clicks_df.count()
                self.logger.info(
                    f"Found {filtered_count} clicks for user agent: {user_agent_filter}")

                clicks_df = self._process_file_type(clicks_df, date)

            # Combine results
            final_df = self._combine_and_fill_hours(
                impressions_df, clicks_df, date)

            # Write output
            self._write_output(final_df.coalesce(1), output_path, date)

            # Cleanup processed files
            for file in impression_files:
                self._cleanup_file(input_path, file)
            for file in click_files:
                self._cleanup_file(input_path, file)

            # everything succeeded, so return True
            return True
        except Exception as e:
            self.logger.error(
                f"Error processing files for date '{date}': {str(e)}")
            raise

    def _validate_dataframe(self, df: DataFrame, expected_schema: StructType, data_type: str) -> bool:
        """Validate DataFrame structure and content."""
        try:
            # Check if DataFrame is empty
            if df.rdd.isEmpty():
                self.logger.warning(f"Empty {data_type} DataFrame")
                return True

            # Validate schema structure
            for field in expected_schema.fields:
                if field.name not in df.columns:
                    self.logger.error(
                        f"Missing required column {field.name} in {data_type}")
                    return False

            # Validate data quality
            null_count = df.filter(
                F.col("device_settings.user_agent").isNull()).count()
            if null_count > 0:
                self.logger.warning(
                    f"Found {null_count} rows with null user_agent in {data_type}")

            return True
        except Exception as e:
            self.logger.error(
                f"Error validating {data_type} DataFrame: {str(e)}")
            return False

    def _process_file_type(self, df: DataFrame, date: str) -> DataFrame:
        """Process either impression or click files with improved hour extraction."""
        if df is None or df.rdd.isEmpty():
            # Return empty DataFrame with correct schema
            schema = StructType([
                StructField("hour", IntegerType(), True),
                StructField("count", IntegerType(), True)
            ])
            return self.spark.createDataFrame([], schema)

        try:
            # Add debug logging for timestamp extraction
            sample_filename = df.select(F.input_file_name()).head(1)[0][0]
            self.logger.info(f"Processing file: {sample_filename}")

            # Extract timestamp with validation
            df_with_hour = df.withColumn(
                "extracted_timestamp",
                F.regexp_extract(F.input_file_name(), r"dk_(\d{14})", 1)
            ).withColumn(
                "hour",
                F.hour(F.to_timestamp(
                    F.col("extracted_timestamp"), 'yyyyMMddHHmmss'))
            )

            # Validate hour extraction
            invalid_hours = df_with_hour.filter(
                (F.col("hour").isNull()) |
                (F.col("hour") < 0) |
                (F.col("hour") > 23)
            ).count()

            self.logger.info(
                f"Found {invalid_hours} records with invalid hours")

            if invalid_hours > 0:
                self.logger.error(
                    f"Found {invalid_hours} records with invalid hours")
                # Filter out invalid records
                df_with_hour = df_with_hour.filter(
                    F.col("hour").isNotNull() &
                    (F.col("hour") >= 0) &
                    (F.col("hour") <= 23)
                )

            # Log hour distribution for debugging
            hour_counts = df_with_hour.groupBy("hour").count()
            self.logger.info(
                f"Hour distribution before aggregation: {hour_counts.collect()}")

            # Group by hour with additional validation
            result_df = df_with_hour.groupBy("hour").agg(
                F.count("*").alias("count"),
                F.count(F.when(F.col("hour").isNotNull(), True)
                        ).alias("valid_count")
            )

            self.logger.info(
                f"Aggregated hour distribution: {result_df.collect()}")

            # Validate counts
            result_df = result_df.select(
                "hour",
                F.when(F.col("count") == F.col("valid_count"), F.col("count"))
                .otherwise(F.col("valid_count"))
                .alias("count")
            )

            # Log the final distribution
            self.logger.info(f"Final hour distribution: {result_df.collect()}")

            return result_df

        except Exception as e:
            self.logger.error(f"Error in _process_file_type: {str(e)}")
            raise

    def _combine_and_fill_hours(self, impressions_df: DataFrame, clicks_df: DataFrame, date: str) -> DataFrame:
        """
        Combine impression and click counts with enhanced formatting.
        Ensures consistent date and time formatting in the output.
        """
        try:
            # Create base hours DataFrame
            hours_df = self.spark.range(0, 24).select(
                F.col("id").alias("hour")
            )

            # Handle case where both DataFrames are empty
            if (impressions_df is None or impressions_df.rdd.isEmpty()) and \
                    (clicks_df is None or clicks_df.rdd.isEmpty()):
                return hours_df.withColumn("date", F.lit(date)) \
                    .withColumn("impression_count", F.lit(0)) \
                    .withColumn("click_count", F.lit(0))

            # Join with impressions
            if impressions_df is not None and not impressions_df.rdd.isEmpty():
                hours_df = hours_df.join(
                    impressions_df,
                    "hour",
                    "left"
                ).withColumnRenamed("count", "impression_count")
            else:
                hours_df = hours_df.withColumn("impression_count", F.lit(0))

            # Join with clicks
            if clicks_df is not None and not clicks_df.rdd.isEmpty():
                hours_df = hours_df.join(
                    clicks_df,
                    "hour",
                    "left"
                ).withColumnRenamed("count", "click_count")
            else:
                hours_df = hours_df.withColumn("click_count", F.lit(0))

            # Fill nulls
            result_df = hours_df.na.fill(0)

            # Handle clicks exceeding impressions
            clicks_exceed_mask = F.col(
                "click_count") > F.col("impression_count")
            if not result_df.filter(clicks_exceed_mask).rdd.isEmpty():
                self.logger.warning(
                    f"Found records where clicks exceed impressions: {date}"
                )
                result_df.filter(clicks_exceed_mask).show()

                # Set impressions equal to clicks where clicks are higher
                result_df = result_df.withColumn(
                    "impression_count",
                    F.when(clicks_exceed_mask, F.col("click_count"))
                    .otherwise(F.col("impression_count"))
                )

            # Add formatted date column first in the output
            return result_df \
                .withColumn("date", F.lit(date)) \
                .select(
                    "date",
                    "hour",
                    "impression_count",
                    "click_count"
                ) \
                .orderBy("hour")

        except Exception as e:
            self.logger.error(f"Error in _combine_and_fill_hours: {str(e)}")
            raise

    def _extract_date_from_filename(self, filename: str) -> str:
        """Extract date in 'YYYY-MM-DD' from filename."""
        try:
            parts = filename.split('_')
            datetime_part = parts[3]  # e.g., 20220527113145108
            date_str = datetime_part[:8]  # 20220527
            date_obj = datetime.strptime(date_str, '%Y%m%d')
            return date_obj.strftime('%Y-%m-%d')  # Return only date part
        except Exception as e:
            self.logger.error(
                f"Error extracting date from filename {filename}: {str(e)}")
            raise

    def _write_output(self, df: DataFrame, output_path: str, date: str) -> str:
        """
        Write the processed DataFrame to a CSV file with proper date formatting.
        """
        try:
            # Create output directory if it doesn't exist
            os.makedirs(output_path, exist_ok=True)

            # Clean date format
            clean_date = date.split()[0] if ' ' in date else date

            # Create output filename
            output_file = os.path.join(
                output_path, f"task1_output_{clean_date}.csv"
            )

            # Convert to pandas with proper datetime formatting
            pandas_df = df.toPandas()

            # Write to CSV ensuring proper column order
            pandas_df.to_csv(
                output_file,
                index=False,
                date_format='%Y-%m-%d'  # Format dates as YYYY-MM-DD
            )

            self.logger.info(f"Successfully wrote output to {output_file}")
            return output_file

        except Exception as e:
            self.logger.error(f"Failed to write output for {date}: {str(e)}")
            import traceback
            self.logger.error(f"Stack trace: {traceback.format_exc()}")

    def _cleanup_file(self, input_path: str, file: str) -> None:
        file_path = os.path.join(input_path, file)
        try:
            os.remove(file_path)
            self.logger.info(f"Removed processed file: {file}")
        except Exception as e:
            self.logger.error(f"Error removing file {file}: {str(e)}")

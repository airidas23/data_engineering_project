import os
import shutil
import tempfile
import unittest
import logging
from datetime import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F

# Adjust the import based on your project structure
from src.data_processing import DataProcessor


class TestDataProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize a local SparkSession for testing
        cls.spark = SparkSession.builder \
            .master("local[*]") \
            .appName("DataProcessorTest") \
            .getOrCreate()
        cls.logger = logging.getLogger("TestDataProcessor")
        cls.logger.setLevel(logging.DEBUG)
        cls.data_processor = DataProcessor(cls.spark, cls.logger)

        # Create temporary directories for input and output
        cls.temp_input_dir = tempfile.mkdtemp()
        cls.temp_output_dir = tempfile.mkdtemp()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        # Cleanup temporary directories
        shutil.rmtree(cls.temp_input_dir)
        shutil.rmtree(cls.temp_output_dir)

    def test_extract_date_from_filename_valid(self):
        filename = "prefix_impressions_processed_dk_20220527113145108.parquet"
        expected_date = "2022-05-27"
        result = self.data_processor._extract_date_from_filename(filename)
        self.assertEqual(result, expected_date)

    def test_extract_date_from_filename_invalid(self):
        filename = "invalid_filename.parquet"
        with self.assertRaises(Exception):
            self.data_processor._extract_date_from_filename(filename)

    def test_validate_dataframe_empty(self):
        schema = StructType(
            [StructField("device_settings.user_agent", StringType(), True)])
        empty_df = self.spark.createDataFrame(
            self.spark.sparkContext.emptyRDD(), schema)
        valid = self.data_processor._validate_dataframe(
            empty_df, schema, "test")
        # Validation should pass for an empty DataFrame with proper schema
        self.assertTrue(valid)

    def test_validate_dataframe_missing_column(self):
        expected_schema = StructType(
            [StructField("device_settings.user_agent", StringType(), True)])
        # Create a DataFrame missing the required column
        schema = StructType(
            [StructField("some_other_column", IntegerType(), True)])
        df = self.spark.createDataFrame(
            [Row(some_other_column=1)], schema=schema)
        valid = self.data_processor._validate_dataframe(
            df, expected_schema, "test")
        self.assertFalse(valid)

    def test_process_file_type_empty(self):
        # Test _process_file_type with an empty DataFrame
        empty_schema = StructType([])
        empty_df = self.spark.createDataFrame(
            self.spark.sparkContext.emptyRDD(), schema=empty_schema)
        result_df = self.data_processor._process_file_type(
            empty_df, "2022-05-27")
        # The result should remain empty
        self.assertTrue(result_df.rdd.isEmpty())

    def test_combine_and_fill_hours_both_empty(self):
        # Create empty DataFrames for impressions and clicks with required schema
        schema = StructType([
            StructField("hour", IntegerType(), True),
            StructField("count", IntegerType(), True)
        ])
        empty_impressions = self.spark.createDataFrame(
            self.spark.sparkContext.emptyRDD(), schema)
        empty_clicks = self.spark.createDataFrame(
            self.spark.sparkContext.emptyRDD(), schema)
        date = "2022-05-27"
        result_df = self.data_processor._combine_and_fill_hours(
            empty_impressions, empty_clicks, date)

        # Collect results and verify that for each hour, counts are zero and date is correctly set
        results = result_df.collect()
        self.assertEqual(len(results), 24)  # 24 hours
        for row in results:
            self.assertEqual(row.date, date)
            self.assertEqual(row.impression_count, 0)
            self.assertEqual(row.click_count, 0)

    def test_process_files_for_user_agent_no_input_path(self):
        # Test when input path does not exist; should return False.
        non_existent_path = os.path.join(self.temp_input_dir, "nonexistent")
        success = self.data_processor.process_files_for_user_agent(
            input_path=non_existent_path,
            output_path=self.temp_output_dir,
            user_agent_filter="test-agent"
        )
        self.assertFalse(success)

    # Additional tests could include more detailed scenarios such as:
    # - Creating dummy parquet files with Spark, running process_files_for_user_agent,
    #   and verifying the output. This often requires setting up temporary files and mocking
    #   parts of the pipeline.
    # - Testing _process_date_files, _write_output, and _cleanup_file with controlled inputs.


if __name__ == "__main__":
    unittest.main()

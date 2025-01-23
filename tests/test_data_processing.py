# import pytest
# from pyspark.sql import functions as F
# from pyspark.sql import SparkSession
# import os
# from pyspark.sql.types import StructType, StructField, StringType, TimestampType, StructType
# import os
# import shutil
# from datetime import datetime
# from src.data_processing import DataProcessor
# # from src.data_validation import DataValidator


# @pytest.fixture(scope="session")
# def spark():
#     """Create a SparkSession that will be used by all tests."""
#     return SparkSession.builder \
#         .master("local[*]") \
#         .appName("TestDataProcessing") \
#         .config("spark.sql.shuffle.partitions", "1") \
#         .config("spark.default.parallelism", "1") \
#         .getOrCreate()


# @pytest.fixture(scope="session", autouse=True)
# def configure_hadoop():
#     """Set HADOOP_HOME and update PATH for Hadoop utilities."""
#     hadoop_home = 'C:\\hadoop'
#     winutils_path = os.path.join(hadoop_home, 'bin')

#     # Set environment variables
#     os.environ['HADOOP_HOME'] = hadoop_home
#     os.environ['PATH'] += os.pathsep + winutils_path

#     # Ensure winutils.exe exists
#     winutils = os.path.join(winutils_path, 'winutils.exe')
#     if not os.path.exists(winutils):
#         raise FileNotFoundError(
#             f"'winutils.exe' not found in {winutils_path}. Please download it and place it there.")


# @pytest.fixture
# def sample_schema():
#     """Define the schema that matches our data structure."""
#     return StructType([
#         StructField("device_settings", StructType([
#             StructField("user_agent", StringType(), True)
#         ]), True)
#     ])


# @pytest.fixture
# def data_processor(spark):
#     """Create a DataProcessor instance for testing."""
#     return DataProcessor(spark)


# def create_test_parquet(spark, data, schema, base_dir, filename, timestamp="20220526113212045"):
#     """Helper function to create test parquet files with proper naming convention.

#     Args:
#         spark: SparkSession instance
#         data: List of data rows
#         schema: DataFrame schema
#         base_dir: Base directory for output
#         filename: Prefix for the output file
#         timestamp: 14-digit Timestamp string for filename
#     """
#     # Create DataFrame
#     df = spark.createDataFrame(data, schema)

#     # Create a temporary directory for the parquet file
#     temp_dir = os.path.join(base_dir, "_temp")
#     os.makedirs(temp_dir, exist_ok=True)

#     # Write parquet file
#     df.write.mode("overwrite").parquet(temp_dir)

#     # Find the part file and copy it to final location with correct name
#     part_files = [f for f in os.listdir(temp_dir) if f.startswith("part-")]
#     if part_files:
#         src_path = os.path.join(temp_dir, part_files[0])
#         dst_path = os.path.join(
#             base_dir, f"{filename}_{timestamp}_172845633-172845636_1.parquet")
#         shutil.copy2(src_path, dst_path)

#     # Clean up temporary directory
#     shutil.rmtree(temp_dir)


# def test_empty_input_directory(data_processor, tmp_path):
#     """Test behavior when input directory is empty."""
#     input_dir = tmp_path / "input"
#     output_dir = tmp_path / "output"
#     input_dir.mkdir()
#     output_dir.mkdir()

#     result = data_processor.process_files_for_user_agent(
#         input_path=str(input_dir),
#         output_path=str(output_dir)
#     )

#     assert not result, "Should return False when no files present"
#     assert len(list(output_dir.iterdir())
#                ) == 0, "No output files should be created"


# def test_invalid_parquet_files(data_processor, tmp_path):
#     """Test handling of corrupted or invalid parquet files."""
#     input_dir = tmp_path / "input"
#     output_dir = tmp_path / "output"
#     input_dir.mkdir()
#     output_dir.mkdir()

#     # Create an invalid parquet file
#     invalid_file = input_dir / \
#         "impressions_processed_dk_20220526113212045_172845633-172845636_1.parquet"
#     with open(invalid_file, 'w') as f:
#         f.write("This is not a parquet file")

#     result = data_processor.process_files_for_user_agent(
#         input_path=str(input_dir),
#         output_path=str(output_dir)
#     )

#     assert not result, "Should handle invalid parquet files gracefully"


# def test_multiple_dates_processing(spark, data_processor, tmp_path, sample_schema):
#     """Test processing of files from multiple dates."""
#     # Create directory structure
#     input_dir = tmp_path / "input"
#     output_dir = tmp_path / "output"
#     input_dir.mkdir()
#     output_dir.mkdir()

#     # Create test data for multiple dates
#     data_day1 = [
#         ({"user_agent": "some user agent"},),
#         ({"user_agent": "some user agent"},)
#     ]
#     data_day2 = [
#         ({"user_agent": "some user agent"},),
#         ({"user_agent": "different agent"},)
#     ]

#     # Create parquet files for different dates
#     create_test_parquet(
#         spark=spark,
#         data=data_day1,
#         schema=sample_schema,
#         base_dir=str(input_dir),
#         filename="impressions_processed_dk",
#         timestamp="20220526113212"
#     )

#     create_test_parquet(
#         spark=spark,
#         data=data_day2,
#         schema=sample_schema,
#         base_dir=str(input_dir),
#         filename="impressions_processed_dk",
#         timestamp="20220527113212"
#     )

#     # Process the files
#     result = data_processor.process_files_for_user_agent(
#         input_path=str(input_dir),
#         output_path=str(output_dir)
#     )

#     assert result, "Processing should complete successfully"

#     # Find CSV output directories (Spark creates directories for CSV output)
#     csv_dirs = [d for d in output_dir.iterdir(
#     ) if d.name.startswith("task1_output_")]
#     assert len(csv_dirs) == 2, "Should create output files for both dates"

#     # Verify content of output files
#     for csv_dir in csv_dirs:
#         # Read the CSV file from the directory
#         df = spark.read.csv(str(csv_dir), header=True)

#         # Basic validations
#         row_count = df.count()
#         assert row_count == 24, f"Should have exactly 24 hours, got {row_count}"

#         # Verify hour column values
#         hours = sorted([int(row.hour) for row in df.select("hour").collect()])
#         assert hours == list(range(24)), "Should contain all hours 0-23"

#         # Verify date format
#         date = df.select("date").first()[0]
#         assert date.startswith("2022-05-"), f"Invalid date format: {date}"

#         # Verify counts are integers
#         for row in df.collect():
#             assert str(row.impression_count).isdigit(
#             ), "Impression count should be an integer"
#             assert str(row.click_count).isdigit(
#             ), "Click count should be an integer"


# def test_user_agent_case_sensitivity(spark, data_processor, tmp_path, sample_schema):
#     """Test case sensitivity in user agent filtering."""
#     input_dir = tmp_path / "input"
#     output_dir = tmp_path / "output"
#     input_dir.mkdir()
#     output_dir.mkdir()

#     data = [
#         ({"user_agent": "Some User Agent"},),
#         ({"user_agent": "SOME USER AGENT"},),
#         ({"user_agent": "some user agent"},)
#     ]

#     create_test_parquet(spark, data, sample_schema,
#                         str(input_dir), "impressions_processed_dk", timestamp="20220526143212045")

#     data_processor.process_files_for_user_agent(
#         input_path=str(input_dir),
#         output_path=str(output_dir),
#         user_agent_filter="some user agent"
#     )

#     # Verify only exact matches are counted
#     output_files = list(output_dir.iterdir())
#     df = spark.read.csv(str(output_files[0]), header=True)
#     total_impressions = sum(int(row.impression_count) for row in df.collect())
#     assert total_impressions == 1, "Should only count exact user agent matches"


# def test_special_characters_in_user_agent(spark, data_processor, tmp_path, sample_schema):
#     """Test handling of special characters in user agent strings."""
#     input_dir = tmp_path / "input"
#     output_dir = tmp_path / "output"
#     input_dir.mkdir()
#     output_dir.mkdir()

#     data = [
#         ({"user_agent": "user-agent/1.0 (special; chars)"},),
#         ({"user_agent": "user-agent\\with\\backslashes"},)
#     ]

#     create_test_parquet(spark, data, sample_schema, str(input_dir))

#     # Test with special characters in filter
#     data_processor.process_files_for_user_agent(
#         input_path=str(input_dir),
#         output_path=str(output_dir),
#         user_agent_filter="user-agent/1.0 (special; chars)"
#     )

#     output_files = list(output_dir.iterdir())
#     df = spark.read.csv(str(output_files[0]), header=True)
#     total_impressions = sum(int(row.impression_count) for row in df.collect())
#     assert total_impressions == 1, "Should handle special characters correctly"


# def test_missing_device_settings(spark, data_processor, tmp_path):
#     """Test handling of records with missing device_settings field."""
#     input_dir = tmp_path / "input"
#     output_dir = tmp_path / "output"
#     input_dir.mkdir()
#     output_dir.mkdir()

#     # Create schema without device_settings
#     simple_schema = StructType([
#         StructField("user_agent", StringType(), True)
#     ])

#     data = [("some user agent",)]

#     df = spark.createDataFrame(data, simple_schema)
#     test_file = input_dir / "test_parquet"
#     df.write.parquet(str(test_file))

#     result = data_processor.process_files_for_user_agent(
#         input_path=str(input_dir),
#         output_path=str(output_dir)
#     )

#     assert not result, "Should handle missing device_settings gracefully"


# def test_cleanup_after_processing(spark, data_processor, tmp_path, sample_schema):
#     """Test that input files are properly cleaned up after processing."""
#     input_dir = tmp_path / "input"
#     output_dir = tmp_path / "output"
#     input_dir.mkdir()
#     output_dir.mkdir()

#     data = [({"user_agent": "some user agent"},)]

#     create_test_parquet(
#         spark, data, sample_schema, str(input_dir),
#         filename="impressions_processed_dk", timestamp="20220526133212"
#     )

#     initial_files = set(os.listdir(input_dir))
#     assert len(initial_files) > 0, "Should have test files created"

#     data_processor.process_files_for_user_agent(
#         input_path=str(input_dir),
#         output_path=str(output_dir)
#     )

#     remaining_files = set(os.listdir(input_dir))
#     assert len(
#         remaining_files) == 0, "All input files should be removed after processing"


# def test_concurrent_date_processing(spark, data_processor, tmp_path, sample_schema):
#     """Test processing of multiple files for the same date."""
#     input_dir = tmp_path / "input"
#     output_dir = tmp_path / "output"
#     input_dir.mkdir()
#     output_dir.mkdir()

#     # Create multiple files for the same date
#     data1 = [({"user_agent": "some user agent"},)]
#     data2 = [({"user_agent": "some user agent"},)]

#     create_test_parquet(
#         spark, data1, sample_schema, str(input_dir),
#         filename="impressions_processed_dk", timestamp="20220526113212"
#     )
#     create_test_parquet(
#         spark, data2, sample_schema, str(input_dir),
#         filename="impressions_processed_dk", timestamp="20220526123212"
#     )

#     data_processor.process_files_for_user_agent(
#         input_path=str(input_dir),
#         output_path=str(output_dir)
#     )

#     # Verify results are properly aggregated
#     output_files = list(output_dir.iterdir())
#     # assert total_impressions == 2, "Should aggregate counts from all files"
#     assert len(output_files) == 1, "Should create single output file for same date"

#     df = spark.read.csv(str(output_files[0]), header=True)
#     total_impressions = sum(int(row.impression_count) for row in df.collect())
#     assert total_impressions == 2, "Should aggregate counts from all files"


# def test_only_clicks_files_processing(spark, data_processor, tmp_path, sample_schema):
#     """Test processing when only click files are present without any impression files."""
#     # Create directory structure
#     input_dir = tmp_path / "input"
#     output_dir = tmp_path / "output"
#     input_dir.mkdir()
#     output_dir.mkdir()
#     # Create test data for clicks
#     click_data = [
#         ({"user_agent": "some user agent"},),
#         ({"user_agent": "some user agent"},),
#         ({"user_agent": "other agent"},)
#     ]
#     # Create a clicks parquet file
#     create_test_parquet(
#         spark=spark,
#         data=click_data,
#         schema=sample_schema,
#         base_dir=str(input_dir),
#         filename="clicks_processed_dk",
#         timestamp="20220528113212"
#     )
#     # Process the files
#     result = data_processor.process_files_for_user_agent(
#         input_path=str(input_dir),
#         output_path=str(output_dir),
#         user_agent_filter="some user agent"
#     )
#     assert result, "Processing should complete successfully with only click files"
#     # Find CSV output directories
#     csv_dirs = [d for d in output_dir.iterdir(
#     ) if d.name.startswith("task1_output_")]
#     assert len(
#         csv_dirs) == 1, "Should create one output file for the clicks date"
#     # Verify content of the output file
#     for csv_dir in csv_dirs:
#         df = spark.read.csv(str(csv_dir), header=True)
#         # Basic validations
#         row_count = df.count()
#         assert row_count == 24, f"Should have exactly 24 hours, got {row_count}"
#         # Verify hour column values
#         hours = sorted([int(row.hour)
#                        for row in df.select("hour").collect()])
#         assert hours == list(range(24)), "Should contain all hours 0-23"
#         # Verify date format
#         date = df.select("date").first()[0]
#         assert date == "2022-05-28", f"Invalid date format: {date}"
#         # Verify counts
#         for row in df.collect():
#             if row.click_count is not None:
#                 assert str(row.click_count).isdigit(
#                 ), "Click count should be an integer"
#             if row.impression_count is not None:
#                 assert int(
#                     row.impression_count) == 0, "Impression count should be zero when no impression files are present"

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

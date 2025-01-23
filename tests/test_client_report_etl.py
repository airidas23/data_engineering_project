import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import logging
from datetime import datetime

from pyspark import Row, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from config import INPUT_PATH, OUTPUT_PATH, LOG_PATH
from src.data_processing import DataProcessor
from src.etl.warehouse import ClientReportETL
from src.utils import get_logger


class NoOpClientReportETL(ClientReportETL):
    """Subclass to override engine creation and _ensure_schema to prevent actual DB schema creation during tests."""

    def __init__(self, connection_string: str, logger: logging.Logger, auto_correct: bool = False):
        # Use a simple engine without pool arguments for SQLite in tests
        self.engine = create_engine(connection_string)
        self.logger = logger
        self.auto_correct = auto_correct
        self._ensure_schema()

    def _ensure_schema(self):
        # Override to do nothing, bypassing schema creation
        pass


class TestClientReportETL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create a dummy logger
        cls.logger = logging.getLogger("TestClientReportETL")
        cls.logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        cls.logger.addHandler(handler)

        # Use an in-memory SQLite database for testing
        cls.connection_string = "sqlite:///:memory:"

        # Instantiate the ETL class without enforcing schema creation
        cls.etl = NoOpClientReportETL(
            cls.connection_string, cls.logger, auto_correct=False)

    def test_validate_data_missing_columns(self):
        # DataFrame missing required columns
        df = pd.DataFrame({
            'some_column': [1, 2, 3]
        })
        result = self.etl.validate_data(df, source_file="test.csv")
        self.assertFalse(result.is_valid)
        self.assertIn("Missing required columns", result.errors[0])

    def test_validate_data_null_values(self):
        # DataFrame with null values
        df = pd.DataFrame({
            'datetime': [None, '2022-01-01 01:00:00'],
            'impression_count': [100, None],
            'click_count': [10, 5]
        })
        result = self.etl.validate_data(df, source_file="test.csv")
        # Without auto-correct, null issues don't invalidate the entire dataset
        self.assertTrue(result.is_valid)
        self.assertTrue(len(result.warnings) > 0)
        self.assertIsNotNone(result.invalid_records)
        self.assertIn('Contains null values',
                      result.invalid_records.iloc[0]['validation_error'])

    def test_validate_data_negative_values(self):
        # DataFrame with negative values
        df = pd.DataFrame({
            'datetime': ['2022-01-01 01:00:00', '2022-01-01 02:00:00'],
            'impression_count': [-10, 50],
            'click_count': [5, -2]
        })
        result = self.etl.validate_data(df, source_file="test.csv")
        # Instead of asserting is_valid false, check for warnings and invalid records
        self.assertTrue(
            any("negative values" in warning for warning in result.warnings))
        self.assertIsNotNone(result.invalid_records)
        # Verify that negative records are captured
        self.assertTrue(
            (result.invalid_records['validation_error'] == 'Contains negative values').any())

    def test_validate_data_clicks_exceed_impressions_no_autocorrect(self):
        # DataFrame where click_count exceeds impression_count, auto_correct disabled
        df = pd.DataFrame({
            'datetime': ['2022-01-01 01:00:00'],
            'impression_count': [50],
            'click_count': [100]
        })
        result = self.etl.validate_data(df, source_file="test.csv")
        # Instead of asserting is_valid to be False, verify warnings and invalid_records
        self.assertTrue(
            any("clicks exceed impressions" in warning for warning in result.warnings))
        self.assertIsNotNone(result.invalid_records)
        self.assertTrue(
            (result.invalid_records['validation_error'] == 'Clicks exceed impressions').all())

    def test_validate_data_clicks_exceed_impressions_autocorrect(self):
        # Test auto_correct functionality when clicks exceed impressions
        etl_autocorrect = NoOpClientReportETL(
            self.connection_string, self.logger, auto_correct=True)
        df = pd.DataFrame({
            'datetime': ['2022-01-01 01:00:00'],
            'impression_count': [50],
            'click_count': [100]
        })
        result = etl_autocorrect.validate_data(df, source_file="test.csv")
        # With auto_correct, problematic_records should be None and validation passes
        self.assertTrue(result.is_valid)
        self.assertIsNone(result.invalid_records)
        # Check that click_count was corrected in the original DataFrame
        self.assertEqual(df.loc[0, 'click_count'], 50)

    def test_prepare_data(self):
        # DataFrame with proper date and hour columns
        data = {
            'date': ['2022-01-01', '2022-01-01'],
            'hour': [1, 2],
            'impression_count': [100, 200],
            'click_count': [10, 20]
        }
        df = pd.DataFrame(data)
        prepared_df = self.etl.prepare_data(df)

        # Check that original 'date' and 'hour' columns are removed
        self.assertNotIn('date', prepared_df.columns)
        self.assertNotIn('hour', prepared_df.columns)

        # Check that required columns exist
        for col in ['datetime', 'impression_count', 'click_count', 'audit_loaded_datetime']:
            self.assertIn(col, prepared_df.columns)

        # Check datetime formatting
        expected_datetimes = ['2022-01-01 01:00:00', '2022-01-01 02:00:00']
        self.assertListEqual(
            prepared_df['datetime'].tolist(), expected_datetimes)

        # Check that counts are integers
        self.assertTrue(pd.api.types.is_integer_dtype(
            prepared_df['impression_count']))
        self.assertTrue(pd.api.types.is_integer_dtype(
            prepared_df['click_count']))


class TestDataProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Start patching subprocess.run
        cls.patcher = patch('subprocess.run', return_value=subprocess.CompletedProcess(args=[], returncode=0))
        cls.mock_subprocess_run = cls.patcher.start()

        try:
            # Setup environment
            cls.setup_java_home()
            cls.setup_hadoop_env()
            cls.ensure_directories()
            # Initialize logger with NullHandler to discard logs
            cls.logger = logging.getLogger("spark_app_logger")
            cls.logger.setLevel(logging.DEBUG)
            cls.logger.addHandler(logging.NullHandler())
        except Exception as e:
            cls.patcher.stop()
            raise e

        # Create a temporary directory for logs
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.log_path = os.path.join(cls.temp_dir.name, "test_logs.log")

        # Initialize logger
        cls.logger = get_logger(cls.log_path)

        # Create SparkSession
        cls.spark = cls.create_spark_session()

        # Initialize DataProcessor
        cls.data_processor = DataProcessor(spark=cls.spark, logger=cls.logger)

    @classmethod
    def tearDownClass(cls):
        """
        Stop SparkSession, patcher, and clean up resources after all tests.
        """
        # Stop SparkSession and patcher
        cls.spark.stop()
        cls.patcher.stop()

        # Remove and close all handlers associated with the logger
        for handler in cls.logger.handlers[:]:
            handler.close()
            cls.logger.removeHandler(handler)

        # Clean up temporary directory
        cls.temp_dir.cleanup()

    @classmethod
    def setup_java_home(cls):
        """Setup JAVA_HOME similar to main.py."""
        if 'JAVA_HOME' not in os.environ:
            # Attempt to set JAVA_HOME from possible paths
            possible_paths = [
                r"C:\Program Files\Java\jdk-11",
                r"C:\Program Files\Java\jdk-8"
                # Add other paths if necessary
            ]
            for path in possible_paths:
                if os.path.exists(path):
                    os.environ['JAVA_HOME'] = path
                    break
            if 'JAVA_HOME' not in os.environ:
                raise EnvironmentError(
                    "JAVA_HOME is not set and couldn't be found automatically"
                )

    @classmethod
    def setup_hadoop_env(cls):
        """Setup Hadoop environment similar to main.py."""
        # Set up base paths
        base_dir = Path(__file__).parent.parent.parent  # Adjust as per directory structure
        hadoop_home = base_dir / "hadoop"

        # Create necessary directories
        os.makedirs(hadoop_home / "bin", exist_ok=True)
        os.makedirs(hadoop_home / "etc" / "hadoop", exist_ok=True)

        # Set environment variables
        os.environ['HADOOP_HOME'] = str(hadoop_home)
        os.environ['PATH'] = f"{hadoop_home}/bin;{os.environ['PATH']}"
        os.environ['HADOOP_CONF_DIR'] = str(hadoop_home / "etc" / "hadoop")

        # Additional Hadoop settings
        os.environ['HADOOP_OPTS'] = "-Djava.library.path=%HADOOP_HOME%\\bin"

        # # Verify winutils.exe
        # winutils_path = hadoop_home / "bin" / "winutils.exe"
        # if not winutils_path.exists():
        #     raise FileNotFoundError(
        #         f"winutils.exe not found at {winutils_path}. "
        #         "Please download it from https://github.com/cdarlint/winutils"
        #     )

    @classmethod
    def ensure_directories(cls):
        """Ensure required directories exist with proper permissions."""
        input_path = Path(INPUT_PATH)  # Ensure INPUT_PATH is correctly imported
        output_path = Path(OUTPUT_PATH)  # Ensure OUTPUT_PATH is correctly imported
        log_path = Path(LOG_PATH)  # Ensure LOG_PATH is correctly imported

        for path in [input_path, output_path, log_path.parent]:
            path.mkdir(parents=True, exist_ok=True)
            # Ensure proper permissions on Windows
            try:
                hadoop_home = os.environ.get('HADOOP_HOME')
                if os.name == "nt" and hadoop_home and (Path(hadoop_home) / "bin" / "winutils.exe").exists():
                    subprocess.run([
                        str(Path(hadoop_home) / "bin" / "winutils.exe"),
                        "chmod", "777", str(path.resolve())
                    ], check=True)
            except Exception as e:
                cls.logger.warning(f"Could not set permissions for {path}: {e}")

    @classmethod
    def create_spark_session(cls):
        """Create and configure Spark session similar to main.py."""
        conf = SparkConf()

        # Memory and executor configurations
        conf.set("spark.driver.memory", "4g")
        conf.set("spark.executor.memory", "4g")
        conf.set("spark.driver.maxResultSize", "2g")

        # Python worker configurations
        conf.set("spark.python.worker.memory", "1g")
        conf.set("spark.python.worker.timeout", "120")
        conf.set("spark.python.worker.reuse", "true")

        # Performance tuning
        conf.set("spark.sql.shuffle.partitions", "2")
        conf.set("spark.default.parallelism", "2")
        conf.set("spark.sql.adaptive.enabled", "true")

        # Windows-specific configurations
        conf.set("spark.driver.host", "localhost")
        conf.set("spark.driver.bindAddress", "localhost")

        # Set Python interpreter to the current Python executable
        conf.set("spark.pyspark.python", sys.executable)
        conf.set("spark.pyspark.driver.python", sys.executable)

        # Set environment variables to ensure Spark uses the correct Python interpreter
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

        # Port and logging settings
        conf.set("spark.ui.port", "4050")
        if os.name == "nt" and os.environ.get('HADOOP_HOME'):
            input_path = Path(INPUT_PATH)  # Ensure INPUT_PATH is correctly imported
            subprocess.run([
                str(Path(os.environ['HADOOP_HOME']) / "bin" / "winutils.exe"),
                "chmod", "777", str(input_path.resolve())
            ], check=True)

        # Create Spark session
        spark = SparkSession.builder \
            .appName("ImpressionClickCounterApp") \
            .config(conf=conf) \
            .master("local[*]") \
            .getOrCreate()

        # Set global log level to ERROR
        spark.sparkContext.setLogLevel("ERROR")

        # Suppress ShutdownHookManager warnings specifically
        jvm = spark._jvm
        shutdown_logger = jvm.org.apache.log4j.LogManager.getLogger(
            "org.apache.spark.util.ShutdownHookManager")
        shutdown_logger.setLevel(jvm.org.apache.log4j.Level.ERROR)

        return spark

    def test_combine_and_fill_hours_clicks_exceed_impressions(self):
        """
        Test the _combine_and_fill_hours method where clicks exceed impressions.
        """
        # Simulate scenario for date 2022-05-27
        date = "2022-05-27"

        # Define schema for impressions DataFrame
        impressions_schema = StructType([
            StructField("hour", IntegerType(), False),
            StructField("count", IntegerType(), False)
        ])

        # Create a DataFrame for impressions with data only for hour 12
        impressions_data = [Row(hour=12, count=10)]
        impressions_df = self.spark.createDataFrame(
            impressions_data, schema=impressions_schema
        )

        # Define schema for clicks DataFrame
        clicks_schema = StructType([
            StructField("hour", IntegerType(), False),
            StructField("count", IntegerType(), False)
        ])

        # Create a DataFrame for clicks with data for hours 11 and 12
        clicks_data = [Row(hour=11, count=10), Row(hour=12, count=20)]
        clicks_df = self.spark.createDataFrame(
            clicks_data, schema=clicks_schema
        )

        # Log DataFrames
        self.logger.debug("Impressions DataFrame:")
        impressions_df.show(truncate=False)

        self.logger.debug("Clicks DataFrame:")
        clicks_df.show(truncate=False)

        # Verify DataFrames are correctly created
        impressions_count = impressions_df.count()
        self.assertEqual(impressions_count, 1, "Impressions DataFrame should contain 1 row.")

        clicks_count = clicks_df.count()
        self.assertEqual(clicks_count, 2, "Clicks DataFrame should contain 2 rows.")

        # Call _combine_and_fill_hours with the simulated data
        try:
            result_df = self.data_processor._combine_and_fill_hours(
                impressions_df=impressions_df,
                clicks_df=clicks_df,
                date=date
            )
        except Exception as e:
            self.fail(f"_combine_and_fill_hours raised an exception: {e}")

        # Log Result DataFrame
        self.logger.debug("Result DataFrame:")
        result_df.show(truncate=False)

        # Collect results into a dictionary for easier assertions
        result = {row['hour']: (row['impression_count'], row['click_count'])
                  for row in result_df.collect()}

        # Expected outcomes:
        # Hour 11: No impressions originally, clicks = 10 -> impressions should be set to 10
        # Hour 12: Impressions = 10, clicks = 20 -> impressions adjusted to 20, clicks = 20
        self.assertIn(11, result, "Hour 11 is missing from the results.")
        self.assertIn(12, result, "Hour 12 is missing from the results.")
        self.assertEqual(result[11], (10, 10), "Hour 11 counts are incorrect.")
        self.assertEqual(result[12], (20, 20), "Hour 12 counts are incorrect.")

        # Check other hours
        for hour in range(0, 24):
            if hour not in [11, 12]:
                self.assertEqual(result.get(hour, (0, 0)), (0, 0),
                                 f"Hour {hour} counts should be (0, 0) but got {result.get(hour)}.")

    def test_combine_and_fill_hours_no_clicks_or_impressions(self):
        # Test when both DataFrames are empty
        date = "2022-05-28"

        # Define schema for impressions and clicks DataFrames
        schema = StructType([
            StructField("hour", IntegerType(), False),
            StructField("count", IntegerType(), False)
        ])

        # Create empty DataFrames
        impressions_df = self.spark.createDataFrame([], schema)
        clicks_df = self.spark.createDataFrame([], schema)

        # Call _combine_and_fill_hours with empty DataFrames
        result_df = self.data_processor._combine_and_fill_hours(
            impressions_df=impressions_df,
            clicks_df=clicks_df,
            date=date
        )

        # Collect results into a dictionary for easier assertions
        result = {row['hour']: (row['impression_count'], row['click_count'])
                  for row in result_df.collect()}

        # All hours should have (0, 0)
        for hour in range(0, 24):
            self.assertEqual(result.get(hour, (0, 0)), (0, 0),
                             f"Hour {hour} counts should be (0, 0) but got {result.get(hour)}.")

    def test_combine_and_fill_hours_only_impressions(self):
        # Simulate scenario where only impressions are present
        date = "2022-05-29"

        # Define schema
        schema = StructType([
            StructField("hour", IntegerType(), False),
            StructField("count", IntegerType(), False)
        ])

        # Create impressions DataFrame
        impressions_data = [Row(hour=10, count=50), Row(hour=14, count=30)]
        impressions_df = self.spark.createDataFrame(
            impressions_data, schema=schema)

        # Create empty clicks DataFrame
        clicks_df = self.spark.createDataFrame([], schema=schema)

        # Call _combine_and_fill_hours
        result_df = self.data_processor._combine_and_fill_hours(
            impressions_df=impressions_df,
            clicks_df=clicks_df,
            date=date
        )

        # Collect results into a dictionary for easier assertions
        result = {row['hour']: (row['impression_count'], row['click_count'])
                  for row in result_df.collect()}

        # Expected outcomes:
        # Hour 10: (50, 0)
        # Hour 14: (30, 0)
        # All others: (0, 0)
        self.assertIn(10, result, "Hour 10 is missing from the results.")
        self.assertIn(14, result, "Hour 14 is missing from the results.")
        self.assertEqual(result[10], (50, 0), "Hour 10 counts are incorrect.")
        self.assertEqual(result[14], (30, 0), "Hour 14 counts are incorrect.")

        # Check other hours
        for hour in range(0, 24):
            if hour not in [10, 14]:
                self.assertEqual(result.get(hour, (0, 0)), (0, 0),
                                 f"Hour {hour} counts should be (0, 0) but got {result.get(hour)}.")

    def test_combine_and_fill_hours_only_clicks(self):
        # Simulate scenario where only clicks are present
        date = "2022-05-30"

        # Define schema
        schema = StructType([
            StructField("hour", IntegerType(), False),
            StructField("count", IntegerType(), False)
        ])

        # Create empty impressions DataFrame
        impressions_df = self.spark.createDataFrame([], schema=schema)

        # Create clicks DataFrame
        clicks_data = [Row(hour=9, count=15), Row(hour=16, count=25)]
        clicks_df = self.spark.createDataFrame(clicks_data, schema=schema)

        # Call _combine_and_fill_hours
        result_df = self.data_processor._combine_and_fill_hours(
            impressions_df=impressions_df,
            clicks_df=clicks_df,
            date=date
        )

        # Collect results into a dictionary for easier assertions
        result = {row['hour']: (row['impression_count'], row['click_count'])
                  for row in result_df.collect()}

        # Expected outcomes:
        # Hour 9: (15, 15)
        # Hour 16: (25, 25)
        # All others: (0, 0)
        self.assertIn(9, result, "Hour 9 is missing from the results.")
        self.assertIn(16, result, "Hour 16 is missing from the results.")
        self.assertEqual(result[9], (15, 15), "Hour 9 counts are incorrect.")
        self.assertEqual(result[16], (25, 25), "Hour 16 counts are incorrect.")

        # Check other hours
        for hour in range(0, 24):
            if hour not in [9, 16]:
                self.assertEqual(result.get(hour, (0, 0)), (0, 0),
                                 f"Hour {hour} counts should be (0, 0) but got {result.get(hour)}.")


if __name__ == "__main__":
    unittest.main()

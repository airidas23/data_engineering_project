import os
import sys
import argparse
from pathlib import Path
import logging
import subprocess
from pyspark.sql import SparkSession
from pyspark import SparkConf

from config import INPUT_PATH, OUTPUT_PATH, LOG_PATH
from src.Task2.warehouse import ClientReportETL

from src.utils import get_logger
from src.Task1.data_processing import DataProcessor
from dotenv import load_dotenv

# Add Python executable directory to PATH
python_dir = os.path.dirname(sys.executable)
os.environ['PATH'] += os.pathsep + python_dir

load_dotenv()


# def setup_java_home():
#     """Verify and setup JAVA_HOME."""
#     if 'JAVA_HOME' not in os.environ:
#         # Try to find Java installation
#         possible_paths = [
#             r"C:\Program Files\Java\jdk-11"
#         ]
#         for path in possible_paths:
#             if os.path.exists(path):
#                 os.environ['JAVA_HOME'] = path
#                 break
#         if 'JAVA_HOME' not in os.environ:
#             raise EnvironmentError(
#                 "JAVA_HOME is not set and couldn't be found automatically")


def setup_hadoop_env():
    """Setup complete Hadoop environment for Windows."""
    # Set up base paths
    base_dir = os.path.dirname(os.path.abspath(__file__))
    hadoop_home = os.path.join(base_dir, "hadoop")

    # Create necessary directories
    os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)
    os.makedirs(os.path.join(hadoop_home, "etc", "hadoop"), exist_ok=True)

    # Set environment variables
    os.environ['HADOOP_HOME'] = hadoop_home
    os.environ['PATH'] = f"{hadoop_home}\\bin;{os.environ['PATH']}"
    os.environ['HADOOP_CONF_DIR'] = os.path.join(hadoop_home, "etc", "hadoop")

    # Additional Hadoop settings
    os.environ['HADOOP_OPTS'] = "-Djava.library.path=%HADOOP_HOME%\\bin"


def create_spark_session():
    """Create and configure Spark session with enhanced settings for both local and Docker environments."""
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

    # Detect if running in Docker
    in_docker = os.path.exists('/.dockerenv')

    if in_docker:
        # Docker-specific configurations
        # Remove or comment out the following lines
        # conf.set("spark.driver.host", "adform_spark")
        # conf.set("spark.driver.bindAddress", "0.0.0.0")
        logging.info("Configuring Spark for Docker environment (local mode)")
    else:
        # Local development configurations
        conf.set("spark.driver.host", "localhost")
        conf.set("spark.driver.bindAddress", "localhost")
        logging.info("Configuring Spark for local environment")

    # Set Python interpreter
    conf.set("spark.pyspark.python", sys.executable)
    conf.set("spark.pyspark.driver.python", sys.executable)
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    # Port and logging settings
    conf.set("spark.ui.port", "4050")

    # Handle Hadoop permissions if needed
    if os.name == "nt" and os.environ.get('HADOOP_HOME'):
        subprocess.run([
            os.path.join(os.environ['HADOOP_HOME'], "bin", "winutils.exe"),
            "chmod", "777", str(Path(INPUT_PATH).resolve())
        ])

    # Create and configure Spark session
    spark = SparkSession.builder \
        .appName("ImpressionClickCounterApp") \
        .config(conf=conf) \
        .master("local[*]") \
        .getOrCreate()

    # Configure logging
    spark.sparkContext.setLogLevel("ERROR")

    # Suppress ShutdownHookManager warnings
    jvm = spark._jvm
    shutdown_logger = jvm.org.apache.log4j.LogManager.getLogger(
        "org.apache.spark.util.ShutdownHookManager")
    shutdown_logger.setLevel(jvm.org.apache.log4j.Level.ERROR)

    # Log Spark configuration for debugging
    logging.info(f"Created Spark session:")
    logging.info(f"  Version: {spark.version}")
    logging.info(f"  Master: {spark.sparkContext.master}")
    logging.info(f"  App Name: {spark.sparkContext.appName}")
    logging.info(
        f"  Driver Host: {conf.get('spark.driver.host') if not in_docker else 'N/A'}")

    return spark


def get_warehouse_connection():
    """
    Get database connection string from environment variables with enhanced logging
    and Docker network support.
    """
    # Get connection parameters from environment
    # Default to 'postgres' for Docker networking
    host = os.getenv('DB_HOST', 'postgres')
    port = os.getenv('DB_PORT', '5432')
    db = os.getenv('POSTGRES_DB', 'adform_db')
    user = os.getenv('POSTGRES_USER', 'adform_user')
    password = os.getenv('POSTGRES_PASSWORD', 'adform_pass')

    # Build connection string
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{db}"

    # Log connection attempt details (without password)
    logging.info(f"Database connection details:")
    logging.info(f"Host: {host}")
    logging.info(f"Port: {port}")
    logging.info(f"Database: {db}")
    logging.info(f"User: {user}")

    # Test if we're running in Docker
    in_docker = os.path.exists('/.dockerenv')
    logging.info(f"Running in Docker container: {in_docker}")

    # If we're in Docker but host is localhost, warn about potential misconfiguration
    if in_docker and host == 'localhost':
        logging.warning("Running in Docker but trying to connect to localhost! "
                        "Should probably be connecting to 'postgres' instead.")

    return connection_string


def ensure_directories():
    """Ensure required directories exist with proper permissions."""
    for path in [INPUT_PATH, OUTPUT_PATH, Path(LOG_PATH).parent]:
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        # Ensure proper permissions on Windows
        try:
            hadoop_home = os.environ.get('HADOOP_HOME')
            if os.name == "nt" and hadoop_home and os.path.exists(os.path.join(hadoop_home, "bin", "winutils.exe")):
                subprocess.run([
                    os.path.join(hadoop_home, "bin", "winutils.exe"),
                    "chmod", "777", str(path)
                ])
        except Exception as e:
            logging.warning(f"Could not set permissions for {path}: {e}")


def process_and_load_data(spark, etl, input_path: str, output_path: str, user_agent: str,
                          logger: logging.Logger) -> bool:
    """
    Process data with Spark and load it into the client reporting table.
    """
    try:
        # First, process with Spark
        processor = DataProcessor(spark, logger)
        spark_success = processor.process_files_for_user_agent(
            input_path=input_path,
            output_path=output_path,
            user_agent_filter=user_agent
        )

        if not spark_success:
            logger.error("Spark processing failed")
            return False

        logger.info("Starting client report loading process...")

        # Process each output file from Spark
        output_dir = Path(output_path)
        files_processed = 0
        total_rows_loaded = 0

        for csv_file in output_dir.glob('task1_output_*.csv'):
            try:
                # Load the file through our ETL process
                load_stats = etl.load_data(csv_file)
                files_processed += 1
                total_rows_loaded += load_stats['loaded_rows']

                logger.info(f"Processed {csv_file}:")
                logger.info(f"- Archived rows: {load_stats['archived_rows']}")
                logger.info(f"- Loaded rows: {load_stats['loaded_rows']}")

            except Exception as e:
                logger.error(f"Error processing {csv_file}: {str(e)}")
                return False

        if files_processed > 0:
            # Verify the load
            logger.info("\nVerifying data load...")
            verification = etl.verify_load()

            logger.info("\nLoad Summary:")
            logger.info(f"Total files processed: {files_processed}")
            logger.info(f"Total rows loaded: {total_rows_loaded}")
            logger.info(f"\nDate Range:")
            logger.info(f"  Start: {verification['date_range']['start']}")
            logger.info(f"  End: {verification['date_range']['end']}")
            logger.info(f"\nTotal Counts:")
            logger.info(
                f"  Impressions: {verification['totals']['impressions']:,}")
            logger.info(f"  Clicks: {verification['totals']['clicks']:,}")

            logger.info("\nClient report loading completed successfully")
            return True
        else:
            logger.error("No files were processed")
            return False

    except Exception as e:

        logger.exception(f"Error in process_and_load_data: {str(e)}")

        return False


def main(args=None):
    try:
        # Setup environment
        # setup_java_home()
        setup_hadoop_env()
        ensure_directories()

        # Parse arguments
        parser = argparse.ArgumentParser(
            description="Spark ETL for processing impressions/clicks and loading to client report."
        )
        parser.add_argument(
            "--user-agent",
            dest="user_agent",
            default="some user agent",
            help="User agent filter string (default: 'some user agent')."
        )
        parsed_args = parser.parse_args(args)

        # Initialize Logger
        logger = get_logger(LOG_PATH)
        logger.info("Starting application...")

        # Convert paths to absolute and normalize
        abs_input_path = os.path.abspath(INPUT_PATH).replace('\\', '/')
        abs_output_path = os.path.abspath(OUTPUT_PATH).replace('\\', '/')

        # Initialize Client Report ETL
        warehouse_connection = get_warehouse_connection()
        etl = ClientReportETL(warehouse_connection, logger)

        # Create Spark session
        spark = None
        try:
            spark = create_spark_session()
            logger.info("Spark Session created successfully")
            logger.info(f"Spark version: {spark.version}")
            logger.info(f"Spark master: {spark.sparkContext.master}")
            logger.info(f"Spark app name: {spark.sparkContext.appName}")

            # Process data and load to client report
            success = process_and_load_data(
                spark=spark,
                etl=etl,
                input_path=abs_input_path,
                output_path=abs_output_path,
                user_agent=parsed_args.user_agent,
                logger=logger
            )

            if success:
                logger.info("Processing and loading completed successfully")
            else:
                logger.error("Processing or loading failed")
                sys.exit(1)

        finally:
            if spark:
                spark.stop()
                logger.info("Spark Session stopped")

    except Exception as e:
        logging.exception(f"Critical error during execution: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

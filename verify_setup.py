#!/usr/bin/env python3

import os
import subprocess
import sys
import logging
import time
from pathlib import Path
import psycopg2
from pyspark.sql import SparkSession
import docker
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SetupVerification:
    def __init__(self):
        self.project_root = Path(__file__).parent
        # Core project structure
        self.required_dirs = [
            'docker/init',
            'hadoop/bin',
            'logs',
            # 'output/raw_data',
            'src/etl',
            'src/models',
            'tests'
        ]
        self.required_files = [
            'docker/init/01-init-db.sql',
            'docker/docker-compose.yml',
            '.env',
            'main.py',
            'requirements.txt'
        ]

    def check_structure(self):
        """Verify basic project structure exists"""
        logger.info("Checking project structure...")

        # Create missing directories
        for dir_path in self.required_dirs:
            full_path = self.project_root / dir_path
            if not full_path.exists():
                full_path.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created directory: {dir_path}")

        # Check required files
        missing_files = [f for f in self.required_files
                         if not (self.project_root / f).exists()]

        if missing_files:
            logger.error(f"Missing files: {missing_files}")
            return False

        return True

    def check_environment(self):
        """Verify Python environment and core dependencies"""
        logger.info("Checking Python environment...")

        try:
            # Verify Python version
            if sys.version_info < (3, 8):
                logger.error("Python 3.8+ required")
                return False

            # Check core dependencies
            for package in ['pyspark', 'pandas', 'psycopg2', 'docker']:
                try:
                    __import__(package)
                except ImportError as e:
                    logger.error(f"Missing package: {package}")
                    return False

            return True

        except Exception as e:
            logger.error(f"Environment check failed: {str(e)}")
            return False

    def check_database(self):
        """Verify database connection and schema"""
        logger.info("Checking database connection...")

        try:
            conn = psycopg2.connect(
                dbname=os.getenv('POSTGRES_DB', 'adform_db'),
                user=os.getenv('POSTGRES_USER', 'adform_user'),
                password=os.getenv('POSTGRES_PASSWORD', 'adform_pass'),
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5433')
            )

            with conn.cursor() as cur:
                # Check if our schema and main table exist
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 
                        FROM information_schema.tables 
                        WHERE table_schema = 'adform_dw' 
                        AND table_name = 'client_report'
                    )
                """)
                if not cur.fetchone()[0]:
                    logger.error("Required database objects not found")
                    return False

            return True

        except Exception as e:
            logger.error(f"Database check failed: {str(e)}")
            return False

    def check_docker(self):
        """
        Verify Docker setup and PostgreSQL container using a simplified and reliable approach.
        This implementation focuses on using docker-compose directly rather than the Docker SDK
        to avoid version compatibility issues.
        """
        logger.info("Checking Docker setup...")

        try:
            # First verify docker is running by using a simple docker command
            try:
                subprocess.run(
                    ['docker', 'ps'],
                    check=True,
                    capture_output=True,
                    text=True
                )
                logger.info("Docker daemon is responsive")
            except subprocess.CalledProcessError as e:
                logger.error("Docker is not running or not accessible")
                logger.error(f"Error: {e.stderr}")
                return False

            # Verify docker-compose is available
            try:
                subprocess.run(
                    ['docker-compose', '--version'],
                    check=True,
                    capture_output=True,
                    text=True
                )
                logger.info("docker-compose is available")
            except subprocess.CalledProcessError:
                logger.error("docker-compose is not installed")
                return False

            # Check if our container is running
            result = subprocess.run(
                ['docker', 'ps', '--filter', 'name=adform_warehouse', '--format', '{{.Status}}'],
                capture_output=True,
                text=True
            )

            container_status = result.stdout.strip()

            if not container_status:
                logger.info("PostgreSQL container not found - starting new container...")
                return self._start_postgres_container()
            elif not container_status.startswith('Up'):
                logger.info("PostgreSQL container is not running - restarting...")
                return self._start_postgres_container()
            else:
                logger.info("PostgreSQL container is running")
                return self._verify_postgres_health()

        except Exception as e:
            logger.error(f"Docker setup check failed: {str(e)}")
            return False

    def _verify_postgres_health(self):
        """
        Helper method to verify PostgreSQL container health by attempting to connect.
        Includes retry logic with a timeout to allow for container startup.
        """
        logger.info("Verifying PostgreSQL health...")
        max_retries = 30
        retry_delay = 2  # seconds

        for attempt in range(max_retries):
            try:
                conn = psycopg2.connect(
                    dbname=os.getenv('POSTGRES_DB', 'adform_db'),
                    user=os.getenv('POSTGRES_USER', 'adform_user'),
                    password=os.getenv('POSTGRES_PASSWORD', 'adform_pass'),
                    host=os.getenv('DB_HOST', 'localhost'),
                    port=os.getenv('DB_PORT', '5433'),
                    connect_timeout=5
                )
                conn.close()
                logger.info("PostgreSQL container is healthy")
                return True
            except psycopg2.Error:
                if attempt < max_retries - 1:
                    logger.info(f"Waiting for PostgreSQL to be ready... ({attempt + 1}/{max_retries})")
                    time.sleep(retry_delay)
                else:
                    logger.error("PostgreSQL health check failed after maximum retries")
                    return False

    def _start_postgres_container(self):
        """
        Helper method to start PostgreSQL container using docker-compose.
        Includes proper cleanup and verification of the container startup.
        """
        try:
            compose_path = self.project_root / 'docker'
            original_dir = os.getcwd()
            os.chdir(str(compose_path))

            try:
                # Stop and remove existing containers
                logger.info("Stopping existing containers...")
                subprocess.run(
                    ['docker-compose', 'down', '-v'],
                    capture_output=True,
                    check=True,
                    timeout=30
                )

                # Start fresh containers
                logger.info("Starting new containers...")
                subprocess.run(
                    ['docker-compose', 'up', '-d', '--force-recreate'],
                    capture_output=True,
                    check=True,
                    timeout=60
                )

                # Verify the container started successfully
                return self._verify_postgres_health()

            finally:
                os.chdir(original_dir)

        except subprocess.TimeoutExpired:
            logger.error("Timeout while managing containers")
            return False
        except subprocess.CalledProcessError as e:
            error_message = e.stderr.decode() if e.stderr else str(e)
            logger.error(f"Failed to manage containers: {error_message}")
            return False
        except Exception as e:
            logger.error(f"Error managing containers: {str(e)}")
            return False

    def check_spark(self):
        """Verify Spark setup with comprehensive error checking"""
        logger.info("Checking Spark setup...")

        try:
            # Set environment variables for Spark
            os.environ['PYSPARK_PYTHON'] = sys.executable
            os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

            # Create Spark session with minimal configuration
            spark = (SparkSession.builder
                     .appName("SetupCheck")
                     .config("spark.driver.host", "localhost")
                     .config("spark.driver.bindAddress", "localhost")
                     .config("spark.ui.port", "4050")
                     .master("local[*]")
                     .getOrCreate())

            try:
                # Perform a simple test
                test_data = [(1, "test")]
                test_df = spark.createDataFrame(test_data, ["id", "value"])
                test_df.count()  # Force computation

                # Test Spark SQL functionality
                test_df.createOrReplaceTempView("test_view")
                spark.sql("SELECT * FROM test_view").count()

                spark.stop()
                logger.info("Spark functionality verified successfully")
                return True

            except Exception as e:
                logger.error(f"Spark operations failed: {str(e)}")
                spark.stop()
                return False

        except Exception as e:
            logger.error(f"Spark session creation failed: {str(e)}")
            logger.info(
                "Please check if Java is installed and JAVA_HOME is set correctly")

    def run_checks(self):
        """Run all verification checks"""
        checks = {
            'Project Structure': self.check_structure,
            'Python Environment': self.check_environment,
            'Docker Setup': self.check_docker,
            'Database': self.check_database,
            'Spark': self.check_spark
        }

        logger.info("\nStarting verification checks...")
        results = {}
        all_passed = True

        for name, check in checks.items():
            try:
                passed = check()
                results[name] = passed
                if not passed:
                    all_passed = False
            except Exception as e:
                logger.error(f"Error in {name}: {str(e)}")
                results[name] = False
                all_passed = False

        # Print results summary
        logger.info("\n=== Verification Results ===")
        for name, passed in results.items():
            status = "✅ PASSED" if passed else "❌ FAILED"
            logger.info(f"{name}: {status}")

        if all_passed:
            logger.info("\n🎉 All checks passed!")
        else:
            logger.error("\n❌ Some checks failed")

        return all_passed


if __name__ == "__main__":
    verifier = SetupVerification()
    success = verifier.run_checks()
    sys.exit(0 if success else 1)

import logging
from pathlib import Path
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import numpy as np
from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass


@dataclass
class ValidationResult:
    """Container for validation results with detailed information about data quality issues."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    invalid_records: Optional[pd.DataFrame] = None


class ClientReportETL:
    def __init__(self, connection_string: str, logger: logging.Logger, auto_correct: bool = False):
        """
        Initialize ETL with database connection and logging.

        Args:
            connection_string: Database connection string
            logger: Logger instance for recording operations
            auto_correct: If True, automatically correct data quality issues where possible
        """
        self.engine = create_engine(
            connection_string,
            pool_size=5,
            pool_recycle=1800
        )
        self.logger = logger
        self.auto_correct = False
        self._ensure_schema()

    def _ensure_schema(self):
        """Ensure required database objects exist."""
        try:
            with self.engine.begin() as conn:
                # Create schema if not exists
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS adform_dw;"))

                # Create main table if not exists
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS adform_dw.client_report (
                        datetime TIMESTAMP NOT NULL,
                        impression_count BIGINT NOT NULL,
                        click_count BIGINT NOT NULL,
                        audit_loaded_datetime TIMESTAMP NOT NULL,
                        PRIMARY KEY (datetime)
                    );
                """))

                # Create archive table if not exists
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS adform_dw.client_report_archive (
                        LIKE adform_dw.client_report INCLUDING ALL
                    );
                """))

                # Create invalid records table for tracking problematic data
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS adform_dw.client_report_invalid (
                        datetime TIMESTAMP NOT NULL,
                        impression_count BIGINT NOT NULL,
                        click_count BIGINT NOT NULL,
                        audit_loaded_datetime TIMESTAMP NOT NULL,
                        validation_error TEXT NOT NULL,
                        source_file TEXT NOT NULL,
                        PRIMARY KEY (datetime, source_file)
                    );
                """))

                # Create index if not exists
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_client_report_datetime
                    ON adform_dw.client_report(datetime);
                """))

                self.logger.info(
                    "Database schema and tables verified/created successfully")

        except SQLAlchemyError as e:
            self.logger.error(f"Error ensuring schema: {str(e)}")
            raise

    def validate_data(self, df: pd.DataFrame, source_file: str) -> ValidationResult:
        """
        Validate the input DataFrame against business rules and data quality checks.

        Args:
            df: Input DataFrame to validate
            source_file: Name of the source file for error tracking

        Returns:
            ValidationResult object containing validation status and details
        """
        errors = []
        warnings = []
        invalid_records = []

        # Check required columns
        required_columns = {'datetime', 'impression_count', 'click_count'}
        missing_columns = required_columns - set(df.columns)
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")
            return ValidationResult(False, errors, warnings)

        # Create a mask for problematic records
        problematic_records = pd.DataFrame()

        # Check for null values
        null_mask = df[list(required_columns)].isnull().any(axis=1)
        if null_mask.any():
            problematic_records = pd.concat([
                problematic_records,
                df[null_mask].assign(validation_error='Contains null values')
            ])
            warnings.append(
                f"Found {null_mask.sum()} records with null values"
            )

        # Check for negative values
        negative_mask = (df['impression_count'] < 0) | (df['click_count'] < 0)
        if negative_mask.any():
            problematic_records = pd.concat([
                problematic_records,
                df[negative_mask].assign(
                    validation_error='Contains negative values'
                )
            ])
            warnings.append(
                f"Found {negative_mask.sum()} records with negative values"
            )

        # Check for clicks exceeding impressions
        invalid_clicks_mask = df['click_count'] > df['impression_count']
        if invalid_clicks_mask.any():
            invalid_clicks = df[invalid_clicks_mask].copy()
            if self.auto_correct:
                # Set clicks equal to impressions where they exceed
                df.loc[invalid_clicks_mask, 'click_count'] = df.loc[
                    invalid_clicks_mask, 'impression_count'
                ]
                warnings.append(
                    f"Auto-corrected {invalid_clicks_mask.sum()} records where "
                    "clicks exceeded impressions."
                )
            else:
                problematic_records = pd.concat([
                    problematic_records,
                    invalid_clicks.assign(
                        validation_error='Clicks exceed impressions'
                    )
                ])
                warnings.append(
                    f"Found {invalid_clicks_mask.sum()} records where clicks "
                    "exceed impressions"
                )

        # Store invalid records if any were found
        if not problematic_records.empty:
            problematic_records['source_file'] = source_file
            problematic_records['audit_loaded_datetime'] = datetime.now()

        # Determine overall validation status
        is_valid = len(errors) == 0
        return ValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            invalid_records=problematic_records if not problematic_records.empty else None
        )

    def _handle_click_impression_mismatch(self, df: pd.DataFrame, source_file: str) -> Tuple[pd.DataFrame, List[Dict]]:
        """
        Handle cases where clicks exceed impressions OR impressions exceed clicks.
        Returns corrected dataframe (if auto_correct is enabled) and list of issues for logging/storing.
        """
        issues = []
        df_copy = df.copy()

        # 1) Identify records with zero impressions but has clicks
        zero_impressions_mask = (df_copy['impression_count'] == 0) & (
            df_copy['click_count'] > 0)
        if zero_impressions_mask.any():
            for _, row in df_copy[zero_impressions_mask].iterrows():
                issues.append({
                    'datetime': row['datetime'],
                    'impression_count': row['impression_count'],
                    'click_count': row['click_count'],
                    'issue_type': 'clicks_without_impressions',
                    'source_file': source_file
                })

            if self.auto_correct:
                # Auto-correct by setting impressions equal to clicks where impressions are zero
                df_copy.loc[zero_impressions_mask,
                            'impression_count'] = df_copy.loc[zero_impressions_mask, 'click_count']
                self.logger.warning(
                    f"Auto-corrected {zero_impressions_mask.sum()} records with clicks but no impressions."
                )

        # 2) Identify records where clicks exceed impressions but impressions are greater than zero
        excess_clicks_mask = (df_copy['click_count'] > df_copy['impression_count']) & \
                             (df_copy['impression_count'] > 0)
        if excess_clicks_mask.any():
            for _, row in df_copy[excess_clicks_mask].iterrows():
                issues.append({
                    'datetime': row['datetime'],
                    'impression_count': row['impression_count'],
                    'click_count': row['click_count'],
                    'issue_type': 'clicks_exceed_impressions',
                    'source_file': source_file
                })

            if self.auto_correct:
                # Auto-correct by setting clicks equal to impressions where clicks exceed impressions
                df_copy.loc[excess_clicks_mask,
                            'click_count'] = df_copy.loc[excess_clicks_mask, 'impression_count']
                self.logger.warning(
                    f"Auto-corrected {excess_clicks_mask.sum()} records where clicks exceeded impressions."
                )

        # 3) Identify records where impressions exceed clicks (if you want to track this discrepancy)
        more_impressions_mask = (df_copy['impression_count'] > df_copy['click_count']) & (
            df_copy['click_count'] > 0)
        if more_impressions_mask.any():
            for _, row in df_copy[more_impressions_mask].iterrows():
                issues.append({
                    'datetime': row['datetime'],
                    'impression_count': row['impression_count'],
                    'click_count': row['click_count'],
                    'issue_type': 'impressions_exceed_clicks',
                    'source_file': source_file
                })
        if issues:
            # Create a DataFrame for the identified issues
            issues_df = pd.DataFrame(issues)
            # Rename 'issue_type' column to 'validation_error' for consistency
            issues_df.rename(
                columns={'issue_type': 'validation_error'}, inplace=True)
            # Ensure 'datetime' column is in datetime format
            issues_df['datetime'] = pd.to_datetime(issues_df['datetime'])
            # Add a timestamp for when the issues were loaded
            issues_df['audit_loaded_datetime'] = datetime.now()

            # Store the invalid records for further analysis
            self.store_invalid_records(issues_df)

        # Return the potentially corrected DataFrame and list of issues
        return df_copy, issues

    def store_invalid_records(self, invalid_records: pd.DataFrame):
        """
        Store invalid records in the invalid records table for later analysis,
        using an UPSERT (merge) approach to avoid duplicates on (datetime, source_file).

        Args:
            invalid_records: DataFrame containing invalid records to be stored. 
                             Expected columns are:
                             - datetime: Timestamp of the record
                             - impression_count: Number of impressions
                             - click_count: Number of clicks
                             - audit_loaded_datetime: Timestamp when the record was loaded
                             - validation_error: Description of the validation error
                             - source_file: Name of the source file from which the record originated
        """
        if invalid_records.empty:
            return

        try:
            with self.engine.begin() as conn:
                # 1) Load into a temporary table (e.g., client_report_invalid_staging).
                #    if_exists='replace' -> creates/replaces the temporary table each time.
                invalid_records.to_sql(
                    'client_report_invalid_staging',
                    conn,
                    schema='adform_dw',
                    if_exists='replace',
                    index=False,
                    method='multi'
                )

                # 2) Perform "upsert" from the temporary table to the main table
                #    POSTGRES syntax: ON CONFLICT (datetime, source_file) DO UPDATE ...
                upsert_sql = text("""
                    INSERT INTO adform_dw.client_report_invalid (
                        datetime,
                        impression_count,
                        click_count,
                        audit_loaded_datetime,
                        validation_error,
                        source_file
                    )
                    SELECT 
                        CAST(s.datetime AS timestamp),
                        s.impression_count,
                        s.click_count,
                        CAST(s.audit_loaded_datetime AS timestamp),
                        s.validation_error,
                        s.source_file
                    FROM adform_dw.client_report_invalid_staging s
                    ON CONFLICT (datetime, source_file)
                    DO UPDATE 
                       SET impression_count     = EXCLUDED.impression_count,
                           click_count         = EXCLUDED.click_count,
                           audit_loaded_datetime = EXCLUDED.audit_loaded_datetime,
                           validation_error    = EXCLUDED.validation_error;
                """)

                # 3) Execute the upsert
                conn.execute(upsert_sql)

                # (Optional) Drop the temporary table
                # If you want to keep the table for debugging, you can leave it
                conn.execute(
                    text("DROP TABLE IF EXISTS adform_dw.client_report_invalid_staging;"))

            self.logger.info(
                f"Upsert completed: stored/updated {len(invalid_records)} invalid records in client_report_invalid."
            )
        except Exception as e:
            self.logger.error(f"Error storing invalid records: {str(e)}")
            raise

    def prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare the DataFrame for loading into the database with proper datetime formatting.

        Args:
            df: Input DataFrame with 'date' and 'hour' columns

        Returns:
            Prepared DataFrame with properly formatted datetime column
        """
        prepared_df = df.copy()

        try:
            # Convert date strings to datetime objects
            prepared_df['date'] = pd.to_datetime(prepared_df['date'])

            # Combine date and hour into a properly formatted datetime
            prepared_df['datetime'] = prepared_df.apply(
                lambda row: pd.Timestamp.combine(
                    row['date'].date(),
                    pd.Timestamp(f"{int(row['hour']):02d}:00:00").time()
                ),
                axis=1
            )

            # Convert datetime to string format
            prepared_df['datetime'] = prepared_df['datetime'].dt.strftime(
                '%Y-%m-%d %H:%M:%S')

            # Drop the original date and hour columns
            prepared_df = prepared_df.drop(['date', 'hour'], axis=1)

            # Convert numeric columns to appropriate types
            prepared_df['impression_count'] = prepared_df['impression_count'].astype(
                'int64')
            prepared_df['click_count'] = prepared_df['click_count'].astype(
                'int64')

            # Add audit timestamp with proper formatting
            prepared_df['audit_loaded_datetime'] = datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S')

            # Sort by datetime for consistent loading
            prepared_df.sort_values('datetime', inplace=True)

            # Ensure we have only the required columns in the correct order
            final_columns = ['datetime', 'impression_count',
                             'click_count', 'audit_loaded_datetime']
            prepared_df = prepared_df[final_columns]

            self.logger.info(
                f"Data prepared successfully. Shape: {prepared_df.shape}")
            return prepared_df

        except Exception as e:
            self.logger.error(f"Error preparing data: {str(e)}")
            self.logger.error("DataFrame columns: " + ", ".join(df.columns))
            self.logger.error("Sample data:\n" + df.head().to_string())
            raise

    def load_data(self, input_path: Path) -> Dict[str, int]:
        """
        Load data from CSV file into the database, accumulating data from multiple files.
        Each file's data is added to the client_report table without overwriting previous data.

        Args:
            input_path: Path to the CSV file to load

        Returns:
            Dictionary containing counts of archived and loaded rows
        """
        try:
            self.logger.info(f"Reading file: {input_path}")

            # Read CSV file
            df = pd.read_csv(input_path)
            self.logger.info(f"Read CSV with shape: {df.shape}")

            # Prepare data
            prepared_df = self.prepare_data(df)

            # Validate the prepared DataFrame to ensure data quality
            validated = self.validate_data(
                prepared_df, source_file=str(input_path))

            # If there are invalid records, store them for further analysis
            if validated.invalid_records is not None:
                self.store_invalid_records(validated.invalid_records)

            # Alternatively, validation can be handled earlier in the data pipeline

            with self.engine.begin() as conn:
                # 1. Archive existing data for the dates in this file
                min_date = prepared_df['datetime'].min()
                max_date = prepared_df['datetime'].max()

                archive_query = text("""
                    INSERT INTO adform_dw.client_report_archive (
                        datetime, impression_count, click_count, audit_loaded_datetime
                    )
                    SELECT cr.datetime, cr.impression_count, cr.click_count, cr.audit_loaded_datetime
                    FROM adform_dw.client_report cr
                    WHERE cr.datetime BETWEEN :min_date AND :max_date
                    AND NOT EXISTS (
                        SELECT 1 
                        FROM adform_dw.client_report_archive ca 
                        WHERE ca.datetime = cr.datetime
                    );
                """)
                archive_result = conn.execute(
                    archive_query,
                    {"min_date": min_date, "max_date": max_date}
                )
                archived_rows = archive_result.rowcount or 0
                self.logger.info(f"Archived {archived_rows} unique rows")

                # 2. Remove existing records for the current date range (to avoid duplicates in client_report)
                delete_query = text("""
                    DELETE FROM adform_dw.client_report
                    WHERE datetime BETWEEN :min_date AND :max_date;
                """)
                conn.execute(delete_query, {
                    "min_date": min_date,
                    "max_date": max_date
                })

                # 3. Insert new data
                prepared_df.to_sql(
                    'client_report',
                    conn,
                    schema='adform_dw',
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )

                # 4. Verify loaded rows
                verify_query = text("""
                    SELECT COUNT(*) 
                    FROM adform_dw.client_report;
                """)
                loaded_rows = conn.execute(verify_query).scalar()
                loaded_rows = loaded_rows if loaded_rows else 0

                self.logger.info(
                    f"Successfully loaded {loaded_rows} rows into client_report.")
                return {
                    'archived_rows': archived_rows,
                    'loaded_rows': loaded_rows
                }

        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            raise

    def verify_load(self) -> dict:
        """Verify the data in client_report and return summary statistics."""
        try:
            with self.engine.connect() as conn:
                # 1. Get total row count
                row_count = conn.execute(text(
                    "SELECT COUNT(*) FROM adform_dw.client_report"
                )).scalar()

                # 2. Get min/max datetime
                date_range = conn.execute(text(
                    """SELECT 
                         MIN(datetime) AS earliest_date,
                         MAX(datetime) AS latest_date
                       FROM adform_dw.client_report"""
                )).fetchone()

                # 3. Get total impressions and clicks
                totals = conn.execute(text(
                    """SELECT 
                         SUM(impression_count) as total_impressions,
                         SUM(click_count) as total_clicks
                       FROM adform_dw.client_report"""
                )).fetchone()

                # 4. Create a dictionary with verification info
                verification = {
                    'total_rows': row_count,
                    'date_range': {
                        'start': date_range.earliest_date,
                        'end': date_range.latest_date
                    },
                    'totals': {
                        'impressions': totals.total_impressions,
                        'clicks': totals.total_clicks
                    }
                }

                # Optionally log or do more checks
                self.logger.info(f"Verification results: {verification}")
                return verification

        except SQLAlchemyError as e:
            self.logger.error(f"Error verifying load: {str(e)}")
            raise

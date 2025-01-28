
import logging
import os
import re
from datetime import datetime


def get_logger(log_path: str) -> logging.Logger:
    """Set up and return a logger instance."""
    logger = logging.getLogger("spark_app_logger")
    logger.setLevel(logging.INFO)
    log_dir = os.path.dirname(log_path)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    # Prevent adding multiple handlers if already exists
    if not logger.handlers:
        file_handler = logging.FileHandler(log_path)
        formatter = logging.Formatter(
            "%(asctime)s - [%(levelname)s] - %(message)s"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger


def parse_datetime_from_filename(filename: str) -> str:
    """
    Extract datetime from filename.
    Example filename: impressions_processed_dk_20220526113212045_172845633-172845636_1.parquet
    We want "2022-05-26 11:32".
    """
    # A naive approach: find the 14 digits after 'dk_': "20220526113212"
    pattern = r"dk_(\d{14})"
    match = re.search(pattern, filename)
    if not match:
        # If you want more robust error handling, raise or return None
        raise ValueError(
            f"Cannot find the date pattern in filename: {filename}")

    dt_str = match.group(1)[:12]  # first 12 digits (YYYYmmddHHMM)
    # dt_str is "YYYYmmddHHMM" -> convert to "YYYY-mm-dd HH:MM"
    dt = datetime.strptime(dt_str, "%Y%m%d%H%M")
    return dt.strftime("%Y-%m-%d %H:%M")

# config.py
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env

INPUT_PATH = os.getenv("INPUT_PATH", "./raw_data")  # Updated input path
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "./output")  # Updated output path
LOG_PATH = os.getenv("LOG_PATH", "./logs/app.log")
APP_ENV = os.getenv("APP_ENV", "dev")

# You can add more environment-specific configs here if needed.

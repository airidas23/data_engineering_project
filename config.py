
import os
from dotenv import load_dotenv

load_dotenv()

INPUT_PATH = os.getenv("INPUT_PATH", "./raw_data")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "./output")
LOG_PATH = os.getenv("LOG_PATH", "./logs/app.log")
APP_ENV = os.getenv("APP_ENV", "dev")



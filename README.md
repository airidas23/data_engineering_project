# Adform Data Engineering Project

## Overview
This project implements a data processing pipeline that handles impression and click data, processes it using Apache Spark, and loads it into a PostgreSQL data warehouse. The pipeline is designed to process parquet files containing advertising impression and click data, aggregate them by hour, and store the results in a structured database format for client reporting.

## Project Structure
```
adform_spark_app/
├── docker/
│   ├── init/
│   │   └── 01-init-db.sql    # Database initialization script
│   └── docker-compose.yml    # Docker services configuration
├── src/
│   ├── etl/
│   │   └── warehouse.py      # ETL processing logic
│   ├── data_processing.py    # Spark data processing
│   └── utils.py             # Utility functions
├── tests/                   # Unit tests
├── .env                     # Environment variables
├── main.py                 # Main application entry point
└── requirements.txt        # Python dependencies
```

## Prerequisites
- Python 3.8 or higher
- Docker Desktop
- Java 11 (for Apache Spark)
- Git

## Setup Instructions

### Windows Setup
1. Install Prerequisites:
   - Install Python 3.8+ from python.org
   - Install Docker Desktop for Windows
   - Install Java 11 JDK
   - Set JAVA_HOME environment variable

2. Clone the repository:
   ```bash
   git clone <repository-url>
   cd adform_spark_app
   ```

3. Create and activate virtual environment:
   ```bash
   python -m venv venv
   .\venv\Scripts\activate
   ```

4. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

5. Configure environment variables:
   Create a .env file with the following content:
   ```
   POSTGRES_USER=adform_user
   POSTGRES_PASSWORD=adform_pass
   POSTGRES_DB=adform_db
   DB_HOST=localhost
   DB_PORT=5433
   ```

### macOS/Linux Setup
1. Install Prerequisites:
   ```bash
   # macOS (using Homebrew)
   brew install python@3.8
   brew install --cask docker
   brew install openjdk@11
   ```

2. Follow steps 2-5 from Windows setup, using appropriate command syntax:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

## Database Setup
The project uses PostgreSQL running in Docker. The database is automatically initialized with the required schema when the container first starts.

1. Start the PostgreSQL container:
   ```bash
   cd docker
   docker-compose up -d
   ```

2. The database initialization script (01-init-db.sql) creates:
   - adform_dw schema
   - client_report table for current data
   - client_report_archive table for historical data
   - Necessary indexes and permissions

## Data Engineering Process

### 1. Data Extraction
- The system monitors the raw_data directory for new parquet files
- Files are categorized into impressions and clicks based on filename patterns
- Each file contains detailed records of individual impressions or clicks
- Files are processed in date order to maintain data consistency

### 2. Data Processing (ETL)
The ETL process consists of several steps:

a) Data Loading:
   - Parquet files are read using PySpark
   - Files are filtered for the specific user agent "some user agent"
   - Timestamp information is extracted from filenames

b) Data Transformation:
   - Data is aggregated by hour for both impressions and clicks
   - Missing hours are filled with zero counts
   - Data quality checks are performed:
     - Validation of hour values
     - Check for clicks exceeding impressions
     - Handling of null values

c) Data Loading:
   - Processed data is written to CSV files
   - Data is loaded into PostgreSQL with proper archiving
   - Input files are removed after successful processing

### 3. Data Warehouse
The data warehouse implementation follows these principles:

- Schema Design:
  ```sql
  CREATE TABLE adform_dw.client_report (
      datetime TIMESTAMP NOT NULL,
      impression_count BIGINT NOT NULL,
      click_count BIGINT NOT NULL,
      audit_loaded_datetime TIMESTAMP NOT NULL,
      PRIMARY KEY (datetime)
  );
  ```

- Data Quality:
  - Primary key constraints prevent duplicate timestamps
  - Archive table maintains historical data
  - Audit timestamps track data lineage

## Running the Application

1. Place input parquet files in the raw_data directory

2. Run the application:
   ```bash
   python main.py --user-agent "some user agent"
   ```

3. The application will:
   - Process all parquet files in raw_data
   - Generate CSV output files
   - Load data into PostgreSQL
   - Remove processed input files

## Monitoring and Logging
- Detailed logs are written to the logs directory
- The application provides progress updates and error reporting
- Database load verification is performed after each processing cycle

## Performance Considerations
- Spark configurations are optimized for local processing
- Database operations use batch loading for efficiency
- Indexes are created on frequently queried columns
- Archive table prevents data loss during updates

## Error Handling
The application includes comprehensive error handling:
- File validation and error reporting
- Database connection retry logic
- Data quality checks with detailed logging
- Automatic cleanup of processed files

## Testing
Run unit tests:
```bash
python -m pytest tests/
```

## Contributing
1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License
[License Information]
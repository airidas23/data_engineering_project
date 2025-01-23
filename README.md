# 🔄 Adform Data Engineering Project

## 📋 Overview
This project implements a **simple data processing pipeline** that handles impression and click data processes. It using **PySpark**, and loads it into a **PostgreSQL**. The pipeline is designed to process parquet files containing advertising impression and click data, aggregate them by hour and store the results in a database for client reporting.

## 🎯 Project Planning
![Project Planning Board](project_planning.png) https://trello.com/invite/b/679246f32031ab106517da0d/ATTI0b7f07f181d94d4a27a8fe722fd5acfcB5A890B5/projectplanning


## 📂 Project Structure
```
adform_spark_app/
├── .cadence/               # Workflow configurations
├── .venv/                  # Python virtual environment
├── .vscode/                # VS Code configuration
├── docker/
│   ├── init/
│   │   └── 01-init-db.sql  # Database initialization script
│   ├── docker-compose.yml  # Docker services configuration
│   └── Dockerfile.spark    # Spark container configuration
├── hadoop/                 # Hadoop configuration for Windows
├── init/                   # Additional initialization scripts
├── jupyter_files/         # Jupyter notebooks for development
├── logs/
│   └── app.log            # Application logging
├── output/                # Processed data output
│   ├── task1_output_2022-05-26
│   ├── task1_output_2022-05-27
│── raw_data/         # Input data directory
├── src/
│   ├── etl/              # ETL processing logic
│   ├
│   └── data_processing.py # Spark data processing
├── tests/                # Unit tests
└── [Configuration Files] # .env, config.py, etc.
```

## 🛠️ Prerequisites
- **Python** 3.8 or higher
- **Docker Desktop**
- **Java 11** (for Apache Spark)
- **Git**

## 🚀 Setup Instructions

### 🪟 Windows Setup
**Install Prerequisites**:
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
   
### 🏃 Running the Application
1. **Place input parquet files** in the raw_data directory
2. **Run the application**:
   ```bash
    python main.py --user-agent "some user agent"
   ```

### 🍎 macOS/Linux Setup
1. Install Prerequisites:
   ```bash
   # macOS (using Homebrew)
   brew install python@3.8
   brew install --cask docker
   brew install openjdk@11
   ```
### 🐧 Linux Deployment
The application can be quickly deployed using our automated deployment script:

1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   cd adform_spark_app
   ```

2. **Make the Deploy Script Executable**:
   ```bash
   chmod +x deploy.sh
   ```

3. **Run the Deployment Script**:
   ```bash
   ./deploy.sh
   ```
   
   To include initial testing, use:
   ```bash
   ./deploy.sh --test
   ```

The deployment script automatically:
- Checks and installs system requirements
- Sets up Python virtual environment
- Installs dependencies
- Configures environment variables
- Starts Docker containers
- Sets up the database

4. **Running the Application**:
   After deployment, run the application using:
   ```bash
   ./main.py --user-agent "some user agent"
   ```

5. **Monitoring Logs**:
   View application logs in real-time:
   ```bash
   tail -f logs/app.log
   ```
## 💾 Data Engineering Process

### 1. 📥 Data Extraction
- **Source**: Raw parquet files
- **Content**: Impressions and clicks data
- **Format**: Parquet

### 2. 🔄 Data Processing (ETL)
- **Technology**: PySpark
- **Processing Steps**:
  - Data validation
  - Aggregation
  - Quality checks

### 3. 📊 Simple Data Warehouse
- **Technology**: PostgreSQL
- **Implementation**: Docker container
- **Schema Design**:
  ```sql
  CREATE TABLE adform_dw.client_report (
      datetime TIMESTAMP NOT NULL,
      impression_count BIGINT NOT NULL,
      click_count BIGINT NOT NULL,
      audit_loaded_datetime TIMESTAMP NOT NULL,
      PRIMARY KEY (datetime)
  );
  ```

## ✅ Task Requirements and Implementation Status

### Task 1: Data Processing 📊
- [x] Create application to calculate impressions and clicks
- [x] Filter data for specific user-agent
- [x] Generate CSV output files
- [x] Fill missing hours with zero values
- [x] Process and remove input files

### Task 2: Database Implementation 💽
- [x] Create PostgreSQL in Docker
- [x] Implement schema and tables
- [x] Data loading application
- [x] Optimization and monitoring

## 📈 Performance Monitoring
- **Logging**: Detailed logs in `/logs/app.log`
- **Coverage report metrics**: 61% Detailed test eeport `htmReport/index.html` 


## 🧪 Testing
```bash
python -m pytest tests/
```

## 🤝 Contributing
1. Fork the repository
2. Create a feature branch
3. Submit a Pull Request

## 📝 Project Progress
To add your project planning visualization:
1. Take a screenshot of your Trello board
2. Save it as `project_planning.png`
3. Place it in the project root directory
4. The image will automatically appear in this section

## 📚 Documentation Resources
- **Resources**: Books, YouTube channels, DataCamp and Codecademy
- **Used Technologies**: Visual Studio Code, Docker, PySpark, PostgreSQL, Copilot, Trello, GitHub, Claude.ai
- **User Guide**: Files will be constantly uploaded to the raw_data directory and processed by the application to generate CSV output files. The data will be loaded into the PostgreSQL database for further analysis.

# Adform Data Engineering Project

## Overview
This project implements a data processing pipeline that handles impression and click data, processes it using Apache Spark, and loads it into a PostgreSQL data warehouse. The pipeline is designed to process parquet files containing advertising impression and click data, aggregate them by hour, and store the results in a structured database format for client reporting.

## Project Structure
```
adform_spark_app/
├── .cadence/               # Workflow configurations
├── .venv/                  # Python virtual environment
├── .vscode/                # VS Code configuration
├── docker/
│   ├── init/
│   │   └── 01-init-db.sql  # Database initialization script
│   ├── docker-compose.yml  # Docker services configuration
│   └── Dockerfile.spark    # Spark container configuration
├── hadoop/                 # Hadoop configuration for Windows
├── init/                   # Additional initialization scripts
├── jupyter_files/         # Jupyter notebooks for development
├── logs/
│   └── app.log            # Application logging
├── output/                # Processed data output
│   ├── task1_output_2022-05-26
│   ├── task1_output_2022-05-27
│   └── raw_data/         # Input data directory
├── src/
│   ├── etl/
│   │   ├── __init__.py
│   │   └── warehouse.py   # ETL processing logic
│   ├── models/
│   │   ├── __init__.py
│   │   ├── base.py       # Base model definitions
│   │   ├── warehouse.py  # Data warehouse models
│   │   └── utils.py      # Model utilities
│   └── __init__.py
│       └── data_processing.py  # Spark data processing
├── tests/
│   ├── __init__.py
│   └── test_data_processing.py
├── .env                   # Environment variables
├── config.py             # Application configuration
├── main.py              # Main application entry point
├── README.md            # Project documentation
└── requirements.txt     # Python dependencies
```

## Task Requirements and Implementation Status

### Task 1: Data Processing
- [x] Create application to calculate impressions and clicks by date and hour
- [x] Filter data for user-agent value "some user agent"
- [x] Process device_settings.user_agent field
- [x] Generate CSV output files with format:
  - [x] Date,hour,impression_count,click_count
  - [x] Example: 2022-05-13,11,255,40
- [x] Fill missing hours with zero values
- [x] Extract datetime from filenames (format: 2022-05-26 11:32)
- [x] Remove processed input files from source directory
- [x] Handle data validation and error checking
- [x] Implement logging and monitoring

### Task 2: Database Implementation
- [x] Create PostgreSQL database in Docker container
- [x] Implement Docker configuration:
  - [x] docker-compose.yml configuration
  - [x] Database initialization scripts
  - [x] Volume management
  - [x] Health checks
- [x] Create database schema:
  - [x] datetime (TIMESTAMP)
  - [x] impression_count (BIGINT)
  - [x] click_count (BIGINT)
  - [x] audit_loaded_datetime (TIMESTAMP)
- [x] Implement data loading application:
  - [x] Read Task 1 output files
  - [x] Transform data for database schema
  - [x] Load data into PostgreSQL
  - [x] Handle data archiving
  - [x] Implement error handling
  - [x] Add data validation
- [x] Create indexes for performance optimization
- [x] Implement data quality checks
- [x] Add monitoring and verification steps

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
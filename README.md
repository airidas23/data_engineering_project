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
1. **Install Prerequisites**:
   ```bash
   # Follow installation steps for each requirement
   ```

2. **Clone & Configure**:
   ```bash
   git clone <repository-url>
   cd adform_spark_app
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


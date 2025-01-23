# Adform Spark ETL Application

## Deployment on Linux

### Prerequisites

- **Docker** and **Docker Compose** installed.
- **Python 3.8+** installed.
- **Java JDK 11** installed and `JAVA_HOME` environment variable set.
- **Hadoop** installed (if required by your Spark setup).

### Steps

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/yourusername/adform_spark_app.git
   cd adform_spark_app

```bash
Configure Environment Variables:

Update the .env file with appropriate Unix-style paths:

INPUT_PATH=/home/rokas/Documents/Airidas/adform/adform_spark_app/raw_data
OUTPUT_PATH=/home/rokas/Documents/Airidas/adform/adform_spark_app/output
LOG_PATH=/home/rokas/Documents/Airidas/adform/adform_spark_app/logs/app.log
APP_ENV=dev

DB_HOST=localhost
DB_PORT=5433
DB_NAME=adform_db
DB_USER=adform_user
DB_PASSWORD=adform_pass

```bash
Build and Start Docker Services:

```bash

docker-compose up -d

Run the Application:

```bash

./deploy.sh

```bash
Monitor Logs:

```bash
tail -f logs/app.log

```bash
Make the deploy.sh script executable:

Run the following command in the terminal:

```bash
chmod +x deploy.sh

```bash
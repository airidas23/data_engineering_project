#!/bin/bash

# Function to verify Java installation
verify_java() {
    echo "Verifying Java installation..."
    if [ -z "$JAVA_HOME" ]; then
        echo "ERROR: JAVA_HOME is not set"
        return 1
    fi

    if [ ! -d "$JAVA_HOME" ]; then
        echo "ERROR: JAVA_HOME directory does not exist: $JAVA_HOME"
        return 1
    fi

    if [ ! -x "$JAVA_HOME/bin/java" ]; then
        echo "ERROR: Java executable not found in: $JAVA_HOME/bin"
        return 1
    fi

    echo "Java version:"
    java -version
    return 0
}

# Function to verify environment
verify_environment() {
    echo "Environment Configuration:"
    echo "========================="
    echo "JAVA_HOME: $JAVA_HOME"
    echo "PYTHONPATH: $PYTHONPATH"
    echo "CRON_ENVIRONMENT: $CRON_ENVIRONMENT"
    echo "DB_HOST: $DB_HOST"
    echo "DB_PORT: $DB_PORT"
}

# Main execution
echo "Starting application setup..."

# Verify Java installation
if ! verify_java; then
    echo "ERROR: Java verification failed"
    exit 1
fi

# Verify environment
verify_environment

# Set up cron schedule
echo "Setting up cron schedule..."
/setup-cron.sh

# Display current cron configuration
echo "Current cron configuration:"
crontab -l

# Start cron service
echo "Starting cron service..."
service cron start

# Run the application once immediately
echo "Running initial application execution..."
cd /app && python main.py

# Start cron and tail the logs
echo "Starting cron job and tailing logs..."
exec tail -f /app/logs/cron.log
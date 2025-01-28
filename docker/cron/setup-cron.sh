#!/bin/bash

# Get the cron schedule based on environment
SCHEDULE=$(python -c "from docker.cron.schedules import get_schedule; print(get_schedule('$CRON_ENVIRONMENT'))")

# Create the cron job file
echo "$SCHEDULE cd /app && JAVA_HOME=${JAVA_HOME} /usr/local/bin/python /app/main.py >> /app/logs/cron.log 2>&1" > /etc/cron.d/app-cron

# Set proper permissions
chmod 0644 /etc/cron.d/app-cron

# Install new cron job
crontab /etc/cron.d/app-cron

echo "Cron schedule set to: $SCHEDULE for environment: $CRON_ENVIRONMENT"
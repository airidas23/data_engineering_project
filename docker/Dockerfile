# Use Python 3.9 slim as base image
FROM python:3.9-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=UTF-8 \
    LANG=C.UTF-8 \
    DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jdk \
    cron \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set up Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH="${JAVA_HOME}/bin:${PATH}"

# Verify Java installation
RUN java -version && \
    echo "JAVA_HOME is set to: ${JAVA_HOME}"

# Set up working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create necessary directories
RUN mkdir -p raw_data output logs hadoop/bin hadoop/etc/hadoop

# Copy cron configuration first
COPY docker/cron /app/docker/cron/

# Set up cron job
COPY docker/cron/setup-cron.sh /setup-cron.sh
RUN chmod +x /setup-cron.sh

# Copy application code
COPY . .

# Copy and set up entrypoint
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
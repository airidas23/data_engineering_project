#!/bin/bash

# Exit on any error
set -e

# Load environment variables if .env exists
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

echo "🚀 Starting Adform ETL application setup..."

# Verify Docker is running
if ! docker info >/dev/null 2>&1; then
    echo "❌ Error: Docker is not running. Please start Docker Desktop first."
    exit 1
fi

# Create required directories
echo "📁 Setting up directory structure..."
directories=("raw_data" "output" "logs" "docker/init")
for dir in "${directories[@]}"; do
    mkdir -p "$dir"
    echo "  ✓ Created/verified $dir"
done

# Ensure database initialization script exists
if [ ! -f "docker/init/01-init-db.sql" ]; then
    echo "❌ Error: Missing docker/init/01-init-db.sql"
    echo "Please ensure the database initialization script is in place."
    exit 1
fi

# Start PostgreSQL container
echo "🐘 Starting PostgreSQL container..."
docker-compose down 2>/dev/null || true
docker-compose up -d

# Wait for PostgreSQL to be ready
echo "⏳ Waiting for PostgreSQL to be ready..."
max_retries=30
counter=0

while ! docker exec adform_warehouse pg_isready -U "${POSTGRES_USER:-adform_user}" -d "${POSTGRES_DB:-adform_db}" >/dev/null 2>&1; do
    counter=$((counter + 1))
    if [ $counter -gt $max_retries ]; then
        echo "❌ Error: PostgreSQL failed to start in time"
        exit 1
    fi
    echo "  Waiting for PostgreSQL... ($counter/$max_retries)"
    sleep 2
done

echo "✅ PostgreSQL is ready!"

# Verify database setup
echo "🔍 Verifying database setup..."
if ! python verify_setup.py; then
    echo "❌ Setup verification failed. Please check the logs."
    exit 1
fi

echo "✨ Setup completed successfully!"
echo "🚀 Starting the main application..."

# Run the main application
python main.py --user-agent "some user agent"
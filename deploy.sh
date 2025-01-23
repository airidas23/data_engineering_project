
#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "Starting deployment..."

# Pull the latest Docker images
docker-compose pull

# Start Docker services
docker-compose up -d

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
until docker exec adform_warehouse pg_isready -U adform_user -d adform_db; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 2
done

echo "PostgreSQL is up - continuing with deployment"

# Activate virtual environment if necessary
# source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run database migrations or initializations if necessary
# python manage.py migrate

# Execute the main application
echo "Running the main application..."
python main.py --user-agent "some user agent"

echo "Deployment completed successfully!"
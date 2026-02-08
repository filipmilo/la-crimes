#!/bin/bash

echo "> Stopping Batch Processing Stack"

# Stop Airflow
echo ">> Stopping Airflow"
docker compose -f Airflow/docker-compose.yml down

# Stop Spark
echo ">> Stopping Apache Spark"
docker compose -f Apache-Spark/docker-compose.yml down

# Stop MongoDB
echo ">> Stopping MongoDB"
docker compose -f MongoDB/docker-compose.yml down

# Optionally remove volumes
read -p "Remove volumes (data will be lost)? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo ">> Removing volumes"
    docker compose -f Airflow/docker-compose.yml down -v
    docker compose -f Apache-Spark/docker-compose.yml down -v
    docker compose -f MongoDB/docker-compose.yml down -v
    echo "   ✓ Volumes removed (MongoDB data deleted)"
fi

echo "✅ Batch Processing Stack stopped"

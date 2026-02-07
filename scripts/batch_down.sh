#!/bin/bash

echo "> Stopping Batch Processing Stack"

# Stop Airflow
echo ">> Stopping Airflow"
docker compose -f Airflow/docker-compose.yml down

# Stop Spark
echo ">> Stopping Apache Spark"
docker compose -f Apache-Spark/docker-compose.yml down

# Stop shared Elasticsearch
echo ">> Stopping Elasticsearch (WARNING: shared with stream processing)"
docker compose -f ElasticSearch/docker-compose.yml down

# Optionally remove volumes
read -p "Remove volumes (data will be lost)? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo ">> Removing volumes"
    docker compose -f Airflow/docker-compose.yml down -v
    docker compose -f Apache-Spark/docker-compose.yml down -v
    docker compose -f ElasticSearch/docker-compose.yml down -v
    echo "   ✓ Volumes removed (gold + stream data deleted)"
fi

echo "✅ Batch Processing Stack stopped"
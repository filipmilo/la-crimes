#!/bin/bash

echo "> Starting Batch Processing Stack"

echo "> Exporting credentials"
export AWS_ACCESS_KEY_ID=$(aws configure get private_account.aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(aws configure get private_account.aws_secret_access_key)
export AWS_DEFAULT_REGION=$(aws configure get private_account.region)

echo "> Creating docker network 'asvsp' (if not exists)"
docker network create asvsp 2>/dev/null || echo "Network 'asvsp' already exists"

echo ">> Starting Elasticsearch"
docker compose -f ElasticSearch/docker-compose.yml up -d

echo ">> Waiting for Elasticsearch to be ready..."
until curl -s http://localhost:9200/_cluster/health | grep -q '"status":"green"\|"status":"yellow"'; do
    echo "   Elasticsearch not ready, waiting..."
    sleep 3
done
echo "   ✓ Elasticsearch ready!"

echo ">> Creating gold layer indices"
bash ElasticSearch/create_indices.sh

echo ">> Starting up Apache Spark"
docker compose -f Apache-Spark/docker-compose.yml up -d
echo ">> Waiting for Spark to be ready..."
until curl -s http://localhost:8000 > /dev/null 2>&1; do
    echo "   Spark not ready, waiting..."
    sleep 3
done
echo "   ✓ Spark ready!"

echo ">> Starting up Airflow"
docker compose -f Airflow/docker-compose.yml up -d
echo ">> Waiting for Airflow to initialize..."
sleep 25

echo ">> Setting up Airflow objects"
cmd='bash -c "/opt/airflow/config/setupObjects.sh"'
docker exec -it airflow-airflow-webserver-1 $cmd

echo ""
echo "✅ Batch Processing Stack started successfully!"
echo ""
echo "Service UIs:"
echo "  - Airflow: http://localhost:8080"
echo "  - Spark Master: http://localhost:8000"
echo "  - Elasticsearch: http://localhost:9200"
echo "  - ES Health: curl http://localhost:9200/_cluster/health"
echo "  - ES Gold Indices: curl http://localhost:9200/_cat/indices/gold-*"
echo "  - ES Stream Indices: curl http://localhost:9200/_cat/indices/911-*"
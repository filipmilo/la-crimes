#!/bin/bash

echo "> Starting Stream Processing Stack"

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

echo ">> Starting up Kafka"
docker compose -f Kafka/docker-compose.yml up -d
echo ">> Waiting for Kafka to be ready..."
sleep 20

echo ">> Starting up Stream Processing"
docker compose -f StreamProcessing/docker-compose.yml up -d
echo ">> Waiting for Stream Processor to initialize..."
sleep 10

echo "✅ Stream Processing Stack started successfully!"
echo ""
echo "Service UIs:"
echo "  - Kafka UI: http://localhost:8081"
echo "  - Elasticsearch: http://localhost:9200"
#!/bin/bash

echo "> Starting Stream Processing Stack"

echo "> Exporting credentials"
export AWS_ACCESS_KEY_ID=$(aws configure get private_account.aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(aws configure get private_account.aws_secret_access_key)
export AWS_DEFAULT_REGION=$(aws configure get private_account.region)

echo "> Creating docker network 'asvsp' (if not exists)"
docker network create asvsp 2>/dev/null || echo "Network 'asvsp' already exists"

echo ">> Starting up Kafka"
docker compose -f Kafka/docker-compose.yml up -d
echo ">> Waiting for Kafka to be ready..."
sleep 20

echo ">> Starting up Stream Processing"
docker compose -f StreamProcessing/docker-compose.yml up -d
echo ">> Waiting for Stream Processor to initialize..."
sleep 10

echo "> Stream Processing Stack started successfully!"
echo "> Kafka UI available at: http://localhost:8081"
echo "> To test: cd Kafka && python producer.py (in one terminal)"
echo ">         cd Kafka && python consumer.py (in another terminal)"
echo ">         cd StreamProcessing && python stream_processor.py (for processing)"
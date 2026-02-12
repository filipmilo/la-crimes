#!/bin/bash

echo "> Starting Stream Processing Stack"

echo "> Creating docker network 'asvsp' (if not exists)"
docker network create asvsp 2>/dev/null || echo "Network 'asvsp' already exists"

echo ">> Starting up Kafka"
docker compose -f Kafka/docker-compose.yml up -d
echo ">> Waiting for Kafka to be ready..."
sleep 20

echo ">> Starting up Kafka Streams Processor"
docker compose -f StreamProcessing/docker-compose.yml up -d
echo ">> Waiting for Stream Processor to initialize..."
sleep 15

echo "✅ Stream Processing Stack started successfully!"
echo ""
echo "Service UIs:"
echo "  - Kafka UI: http://localhost:8081"
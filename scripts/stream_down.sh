#!/bin/bash

echo "> Bringing down Stream Processing Stack"

read -p "> Delete volumes? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
  echo ">> Shutting down Stream Processing"
  docker compose -f StreamProcessing/docker-compose.yml down -v

  echo ">> Shutting down Kafka"
  docker compose -f Kafka/docker-compose.yml down -v
else
  echo ">> Shutting down Stream Processing"
  docker compose -f StreamProcessing/docker-compose.yml down

  echo ">> Shutting down Kafka"
  docker compose -f Kafka/docker-compose.yml down
fi

echo "> Stream Processing Stack stopped"
echo "> Note: Docker network 'asvsp' preserved for other services"
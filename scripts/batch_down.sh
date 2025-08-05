#!/bin/bash

echo "> Bringing down Batch Processing Stack"

read -p "> Delete volumes? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
  echo ">> Shutting down Airflow"
  docker compose -f Airflow/docker-compose.yml down -v

  echo ">> Shutting down Apache Spark"
  docker compose -f Apache-Spark/docker-compose.yml down -v
else
  echo ">> Shutting down Airflow"
  docker compose -f Airflow/docker-compose.yml down

  echo ">> Shutting down Apache Spark"
  docker compose -f Apache-Spark/docker-compose.yml down
fi

echo "> Batch Processing Stack stopped"
echo "> Note: Docker network 'asvsp' preserved for other services"
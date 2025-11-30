#!/bin/bash

echo "> Starting Batch Processing Stack"

echo "> Exporting credentials"
export AWS_ACCESS_KEY_ID=$(aws configure get private_account.aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(aws configure get private_account.aws_secret_access_key)
export AWS_DEFAULT_REGION=$(aws configure get private_account.region)

echo "> Creating docker network 'asvsp' (if not exists)"
docker network create asvsp 2>/dev/null || echo "Network 'asvsp' already exists"

echo ">> Starting up Apache Spark"
docker compose -f Apache-Spark/docker-compose.yml up -d
echo ">> Waiting for Spark to be ready..."
sleep 15

echo ">> Starting up Airflow"
docker compose -f Airflow/docker-compose.yml up -d
echo ">> Waiting for Airflow to initialize..."
sleep 25

echo ">> Setting up Airflow objects"
cmd='bash -c "/opt/airflow/config/setupObjects.sh"'
docker exec -it airflow-airflow-webserver-1 $cmd

echo "> Batch Processing Stack started successfully!"
echo "> Airflow UI available at: http://localhost:8080"
echo "> Spark Master UI available at: http://localhost:8000"
echo "> To test: Access Airflow UI and trigger 'load_dataset' then 'clean_dataset' DAGs"
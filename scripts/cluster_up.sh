#!/bin/bash

echo "> Exporting credentials"

export AWS_ACCESS_KEY_ID=$(aws configure get private_account.aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(aws configure get private_account.aws_secret_access_key)
export AWS_DEFAULT_REGION=$(aws configure get private_account.region)

echo "> Starting up cluster"
echo "> Creating docker network 'asvsp'"
docker network create asvsp

ssh_server_startup_cmd='bash -c "/usr/sbin/sshd"'

echo ">> Starting up Apache Spark"
docker compose -f Apache-Spark/docker-compose.yml up -d
sleep 15
docker exec -it spark-master $ssh_server_startup_cmd

echo ">> Starting up Airflow"
docker compose -f Airflow/docker-compose.yml up -d
sleep 25
echo ">> Setting up Airflow objects"
cmd='bash -c "/opt/airflow/config/setupObjects.sh"'
docker exec -it airflow-airflow-webserver-1 $cmd

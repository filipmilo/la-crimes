#!/bin/bash

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

#echo ">> Starting up Metabase"
#docker compose -f Metabase/docker-compose.yml up -d

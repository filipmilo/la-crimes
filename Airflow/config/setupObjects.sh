#!/bin/bash

# Set up connections
echo ">>> Setting up Airflow connections"

airflow connections add 'AIRFLOW_DB_CONNECTION' \
	--conn-json '{
        "conn_type": "postgres",
        "login": "airflow",
        "password": "airflow",
        "host": "postgres",
        "port": 5432,
        "schema": "airflow"
    }'

airflow connections add 'SPARK_CONNECTION' \
	--conn-json '{
        "conn_type": "spark",
        "host": "spark://spark-master",
        "port": 7077
    }'

airflow connections add 'LOCAL_FS_FILES' \
	--conn-json '{
        "conn_type": "fs",
        "extra": "{ \"path\": \"/opt/airflow/files\"}"
    }'

airflow connections add 'AWS_S3' \
	--conn-json '{
        "conn_type": "aws",
        "extra": "{ \"region_name\": \"us-east-1\", \"profile_name\": \"private_account\"}"
    }'

from datetime import datetime
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(
    dag_id="business_metrics",
    description="DAG to generate business metrics with resolution rates, risk scores, and YoY trends",
    start_date=datetime(2025, 2, 22),
    schedule_interval=None,
)
def perform():
    operator = SparkSubmitOperator(
            task_id='business_metrics',
            conn_id='SPARK_CONNECTION',
            application='dags/jobs/business_metrics.py',
            packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0",
            verbose=True
        )

    return operator


perform()

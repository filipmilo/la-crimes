from datetime import datetime
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(
    dag_id="geographic_enrichment",
    description="DAG to enrich data with geographic zones and location analysis",
    start_date=datetime(2025, 2, 22),
    schedule_interval=None,
)
def perform():
    operator = SparkSubmitOperator(
            task_id='geographic_enrichment',
            conn_id='SPARK_CONNECTION',
            application='dags/jobs/geographic_enrichment.py',
            packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0",
            verbose=True
        )

    return operator


perform()

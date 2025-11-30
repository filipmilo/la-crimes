from datetime import datetime
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(
    dag_id="clean_dataset",
    description="DAG in charge of loading and transforming dataset",
    start_date=datetime(2025, 2, 22),
    schedule_interval=None,
)
def clean_dataset():
    clean_operator = SparkSubmitOperator(
            task_id='clean_dataset',
            conn_id='SPARK_CONNECTION',
            application='dags/jobs/clean_dataset.py',
            packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            verbose=True
        )

    return clean_operator


clean_dataset()

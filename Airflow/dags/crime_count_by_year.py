from datetime import datetime
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(
    dag_id="crime_count_by_year",
    description="DAG used to group crimes by year",
    start_date=datetime(2025, 2, 22),
    schedule_interval=None,
)
def perform():
    operator = SparkSubmitOperator(
            task_id='crime_count_by_year',
            conn_id='SPARK_CONNECTION',
            application='dags/jobs/crime_count_by_year.py',
            packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            verbose=True
        )

    operator


perform()

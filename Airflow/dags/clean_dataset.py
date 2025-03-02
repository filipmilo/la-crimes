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
            application='opt/airflow/dags/jobs/clean_dataset.py',
            conf={
                "spark.master": "spark://spark-master:7077"
            },
            executor_memory="1g",
            driver_memory="1g",
            verbose=True
        )

    clean_operator


clean_dataset()

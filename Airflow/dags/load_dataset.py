from datetime import datetime
from airflow.decorators import dag
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

BUCKET = "la-crimes-data-lake"

@dag(
    dag_id="load_dataset",
    description="DAG in charge of loading data to Amazon S3 bronze layer",
    start_date=datetime(2025, 2, 22),
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
)
def load_dataset():
    create_object = S3CreateObjectOperator(
        task_id="create_object",
        s3_bucket=BUCKET,
        s3_key="bronze",
        data="data/crime_data.csv",
        replace=True,
    )

    print(create_object)


load_dataset()

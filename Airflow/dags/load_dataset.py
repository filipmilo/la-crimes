from datetime import datetime
from airflow.decorators import dag
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

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
    create_object = LocalFilesystemToS3Operator(
        aws_conn_id="AWS_S3",
        task_id="create_object",
        dest_bucket=BUCKET,
        dest_key="bronze/crime_data.csv",
        filename="/opt/airflow/files/crime_data.csv",
        replace=True,
    )

    create_object.run


load_dataset()

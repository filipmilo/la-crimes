from datetime import datetime
from airflow.decorators import dag
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

BUCKET = "la-crimes-data-lake"
SPARK_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

@dag(
    dag_id="pipeline",
    description="DAG in charge of orchestrating the whole pipeline",
    start_date=datetime(2026, 2, 2),
    schedule_interval=None,
)
def pipeline():

    load_dataset = LocalFilesystemToS3Operator(
        aws_conn_id="AWS_S3",
        task_id="create_object",
        dest_bucket=BUCKET,
        dest_key="bronze/crime_data.csv",
        filename="/opt/airflow/files/crime_data.csv",
        replace=True,
    )

    clean_dataset = SparkSubmitOperator(
            task_id='clean_dataset',
            conn_id='SPARK_CONNECTION',
            application='dags/jobs/clean_dataset.py',
            packages=SPARK_PACKAGES,
            verbose=True
        )


    crime_count_by_year = SparkSubmitOperator(
            task_id='crime_count_by_year',
            conn_id='SPARK_CONNECTION',
            application='dags/jobs/crime_count_by_year.py',
            packages=SPARK_PACKAGES,
            verbose=True
        )

    business_metrics = SparkSubmitOperator(
            task_id='business_metrics',
            conn_id='SPARK_CONNECTION',
            application='dags/jobs/business_metrics.py',
            packages=SPARK_PACKAGES,
            verbose=True
        )

    crime_categorization = SparkSubmitOperator(
            task_id='crime_categorization',
            conn_id='SPARK_CONNECTION',
            application='dags/jobs/crime_categorization.py',
            packages=SPARK_PACKAGES,
            verbose=True
        )

    categorical_standardization = SparkSubmitOperator(
            task_id='categorical_standardization',
            conn_id='SPARK_CONNECTION',
            application='dags/jobs/categorical_standardization.py',
            packages=SPARK_PACKAGES,
            verbose=True
        )

    geographic_enrichment = SparkSubmitOperator(
            task_id='geographic_enrichment',
            conn_id='SPARK_CONNECTION',
            application='dags/jobs/geographic_enrichment.py',
            packages=SPARK_PACKAGES,
            verbose=True
        )

    time_dimensions = SparkSubmitOperator(
            task_id='time_dimensions',
            conn_id='SPARK_CONNECTION',
            application='dags/jobs/time_dimensions.py',
            packages=SPARK_PACKAGES,
            verbose=True
        )

    data_quality = SparkSubmitOperator(
            task_id='data_quality',
            conn_id='SPARK_CONNECTION',
            application='dags/jobs/data_quality.py',
            packages=SPARK_PACKAGES,
            verbose=True
        )

    load_dataset >> clean_dataset >> [
        crime_count_by_year,
        business_metrics,
        crime_categorization,
        categorical_standardization,
        geographic_enrichment,
        time_dimensions,
        data_quality
    ]

pipeline()

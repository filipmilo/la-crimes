from datetime import datetime
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"

@dag(
    dag_id="business_metrics",
    description="DAG to generate business metrics with resolution rates, risk scores, and YoY trends",
    start_date=datetime(2025, 2, 22),
    schedule_interval=None,
)
def perform():
    def spark_task(task_id, application):
        return SparkSubmitOperator(
            task_id=task_id,
            conn_id='SPARK_CONNECTION',
            application=application,
            packages=PACKAGES,
            verbose=True
        )

    business_metrics = spark_task('business_metrics', 'dags/jobs/business_metrics.py')
    yoy_crime_trends = spark_task('bm_yoy_crime_trends', 'dags/jobs/bm_yoy_crime_trends.py')
    area_summary     = spark_task('bm_area_summary',     'dags/jobs/bm_area_summary.py')
    reporting_lag    = spark_task('bm_reporting_lag',    'dags/jobs/bm_reporting_lag.py')
    reporting_speed  = spark_task('bm_reporting_speed',  'dags/jobs/bm_reporting_speed.py')

    business_metrics >> [yoy_crime_trends, area_summary, reporting_lag, reporting_speed]


perform()

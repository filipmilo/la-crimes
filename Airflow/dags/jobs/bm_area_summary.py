import sys
sys.path.insert(0, '/opt/airflow/dags')

from spark_config import create_spark_session_with_mongo, S3_BUCKET

spark = create_spark_session_with_mongo("BM Area Summary")

spark.read.parquet(f"s3a://{S3_BUCKET}/silver/business_metrics/area_summary.parquet") \
    .write \
    .format("mongodb") \
    .option("collection", "gold_bm_area_summary") \
    .mode("overwrite") \
    .save()

spark.stop()

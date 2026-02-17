import sys
sys.path.insert(0, '/opt/airflow/dags')

from pyspark.sql.functions import col, avg, count, when, round as spark_round
from spark_config import create_spark_session_with_mongo, S3_BUCKET

spark = create_spark_session_with_mongo("BM Reporting Lag")

spark.read.parquet(f"s3a://{S3_BUCKET}/silver/business_metrics/enriched.parquet") \
    .filter(col("reporting_lag_days").between(0, 365)) \
    .groupBy("area_name") \
    .agg(
        spark_round(avg("reporting_lag_days"), 1).alias("avg_lag_days"),
        spark_round(
            count(when(col("reporting_lag_days") > 30, True)) / count("*") * 100, 1
        ).alias("pct_delayed"),
        count("*").alias("total_records")
    ) \
    .orderBy(col("avg_lag_days").desc()) \
    .write \
    .format("mongodb") \
    .option("collection", "gold_bm_reporting_lag") \
    .mode("overwrite") \
    .save()

spark.stop()

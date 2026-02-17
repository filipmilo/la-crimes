import sys
sys.path.insert(0, '/opt/airflow/dags')

from pyspark.sql.functions import col, count, round as spark_round, sum as spark_sum, lit
from pyspark.sql.window import Window
from spark_config import create_spark_session_with_mongo, S3_BUCKET

spark = create_spark_session_with_mongo("BM Reporting Speed")

total_window = Window.orderBy(lit(1)).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

spark.read.parquet(f"s3a://{S3_BUCKET}/silver/business_metrics/enriched.parquet") \
    .groupBy("reporting_speed") \
    .agg(count("*").alias("count")) \
    .withColumn("pct", spark_round(col("count") / spark_sum("count").over(total_window) * 100, 1)) \
    .write \
    .format("mongodb") \
    .option("collection", "gold_bm_reporting_speed") \
    .mode("overwrite") \
    .save()

spark.stop()

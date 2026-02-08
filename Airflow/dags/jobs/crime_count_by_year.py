import sys
sys.path.insert(0, '/opt/airflow/dags')

from pyspark.sql.functions import year, current_timestamp
from spark_config import create_spark_session_with_mongo, S3_BUCKET

spark = create_spark_session_with_mongo("Crime Count by Year")

df = spark.read.parquet(f"s3a://{S3_BUCKET}/silver/cleaned.parquet")

df = df.groupBy(year("date_occured").alias("year")).count().orderBy("year")

df = df.withColumn("indexed_at", current_timestamp())

df.show()

df.write \
    .format("mongodb") \
    .option("collection", "gold_crime_count_by_year") \
    .mode("overwrite") \
    .save()

spark.stop()

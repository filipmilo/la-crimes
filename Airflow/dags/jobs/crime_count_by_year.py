import sys
sys.path.insert(0, '/opt/airflow/dags')

from pyspark.sql.functions import year
from spark_config import create_spark_session, S3_BUCKET

spark = create_spark_session("Crime Count by Year")

df = spark.read.parquet(f"s3a://{S3_BUCKET}/silver/cleaned.parquet")

df = df.groupBy(year("date_occured").alias("year")).count().orderBy("year")

df.show()

df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(f"s3a://{S3_BUCKET}/gold/crime_count_by_year.parquet")

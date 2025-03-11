import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year


def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("Pyspark clean dataset") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

#quiet_logs(spark)

df = spark.read.parquet("s3a://la-crimes-data-lake/silver/cleaned.parquet")

df.groupBy(year("date_occured").alias("year")).count().orderBy("year").show()

df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet("s3a://la-crimes-data-lake/gold/crime_count_by_year.parquet")

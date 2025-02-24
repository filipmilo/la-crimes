import os
from pyspark.sql import SparkSession

#def quiet_logs(sc):
#  logger = sc._jvm.org.apache.log4j
#  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
#  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("Pyspark clean dataset") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

#quiet_logs(spark)

df = spark.read.csv("s3a://la-crimes-data-lake/bronze/crime_data.csv", header=True, inferSchema=True)

# TODO: Clean data and prep for aggregating phase
df.show(5)


df.write \
    .mode("overwrite") \
    .parquet("s3a://la-crimes-data-lake/silver/")

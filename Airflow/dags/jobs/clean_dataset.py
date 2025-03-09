import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, to_timestamp, date_format, lpad, concat_ws
TIME_FORMAT="yyyy MMM dd hh:mm:ss a"

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

df = spark.read.csv("s3a://la-crimes-data-lake/bronze/crime_data.csv", header=True, inferSchema=True)

df = df \
    .withColumnRenamed("DR_NO", "id") \
    .withColumnRenamed("AREA", "area_id") \
    .withColumnRenamed("AREA_NAME", "area_name") \
    .withColumnRenamed("Rpt Dist No", "reporting_discrict_id") \
    .withColumnRenamed("Part 1-2", "part_1_2") \
    .withColumnRenamed("Crm Cd", "crime_code") \
    .withColumnRenamed("Crm Cd Desc", "crime_description") \
    .withColumnRenamed("Mocodes", "mocodes") \
    .withColumnRenamed("Vict Age", "victim_age") \
    .withColumnRenamed("Vict Sex", "victim_sex") \
    .withColumnRenamed("Vict Descent", "victim_descent") \
    .withColumnRenamed("Premis Cd", "premise_code") \
    .withColumnRenamed("Premis Desc", "premise_description") \
    .withColumnRenamed("Weapon Used Cd", "weapon_code") \
    .withColumnRenamed("Weapon Desc", "weapon_description") \
    .withColumnRenamed("Status", "status") \
    .withColumnRenamed("Status Desc", "status_description") \
    .withColumnRenamed("Crm Cd 1", "crime_code_1") \
    .withColumnRenamed("Crm Cd 2", "crime_code_2") \
    .withColumnRenamed("Crm Cd 3", "crime_code_3") \
    .withColumnRenamed("Crm Cd 4", "crime_code_4") \
    .withColumnRenamed("LOCATION", "street_address") \
    .withColumnRenamed("Cross Street", "cross_street") \
    .withColumnRenamed("LAT", "lat") \
    .withColumnRenamed("LON", "lon") \
    .withColumnRenamed("Date Rptd", "date_reported") \
    .withColumnRenamed("Date OCC", "date_occured") \
    .withColumnRenamed("TIME OCC", "time_occured") 

df = df \
    .withColumn("date_reported", to_date(df["date_reported"], TIME_FORMAT)) \
    .withColumn("date_occured", to_date(df["date_occured"], TIME_FORMAT))


df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet("s3a://la-crimes-data-lake/silver/cleaned.parquet")

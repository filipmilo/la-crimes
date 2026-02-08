import sys
sys.path.insert(0, '/opt/airflow/dags')

from pyspark.sql.functions import to_date, year, col
from spark_config import create_spark_session, S3_BUCKET


'''2010 Jan 26 12:00:00 AM'''
TIME_FORMAT="yyyy MMM dd hh:mm:ss a"

spark = create_spark_session("Clean Dataset")

df = spark.read.csv(f"s3a://{S3_BUCKET}/bronze/crime_data.csv", header=True, inferSchema=True)

df = df \
    .withColumnRenamed("DR_NO", "id") \
    .withColumnRenamed("AREA ", "area_id") \
    .withColumnRenamed("AREA NAME", "area_name") \
    .withColumnRenamed("Rpt Dist No", "reporting_district_id") \
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
    .withColumnRenamed("DATE OCC", "date_occured") \
    .withColumnRenamed("TIME OCC", "time_occured") 

df = df \
    .withColumn("date_reported", to_date(df["date_reported"], TIME_FORMAT)) \
    .withColumn("date_occured", to_date(df["date_occured"], TIME_FORMAT)) \
    .withColumn("occurrence_year", year(col("date_occured"))) \
    .withColumn("report_year", year(col("date_reported")))

df.show(5)

df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(f"s3a://{S3_BUCKET}/silver/cleaned.parquet")

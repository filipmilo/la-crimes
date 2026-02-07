import sys
sys.path.insert(0, '/opt/airflow/dags')

from pyspark.sql.functions import (
    col, year, month, dayofmonth, hour, date_format,
    dayofweek, when, to_timestamp, concat, lpad, lit, current_timestamp
)
from spark_config import create_spark_session_with_es, S3_BUCKET

spark = create_spark_session_with_es("Time Dimensions")

df = spark.read.parquet(f"s3a://{S3_BUCKET}/silver/cleaned.parquet")

# Create full datetime from date and time
df = df.withColumn("datetime_occurred", 
    to_timestamp(
        concat(
            date_format(col("date_occured"), "yyyy-MM-dd"),
            lit(" "),
            lpad(col("time_occured").cast("string"), 4, "0")
        ), 
        "yyyy-MM-dd HHmm"
    )
)

# Extract year, month, day, hour
df = df \
    .withColumn("occurrence_year", year(col("date_occured"))) \
    .withColumn("occurrence_month", month(col("date_occured"))) \
    .withColumn("occurrence_day", dayofmonth(col("date_occured"))) \
    .withColumn("occurrence_hour", hour(col("datetime_occurred")))

# Create time of day categories
df = df.withColumn("time_of_day",
    when((col("occurrence_hour") >= 6) & (col("occurrence_hour") < 12), "Morning")
    .when((col("occurrence_hour") >= 12) & (col("occurrence_hour") < 18), "Afternoon")
    .when((col("occurrence_hour") >= 18) & (col("occurrence_hour") < 22), "Evening")
    .otherwise("Night")
)

# Add day of week and weekend flag
df = df \
    .withColumn("day_of_week", date_format(col("date_occured"), "EEEE")) \
    .withColumn("day_of_week_num", dayofweek(col("date_occured"))) \
    .withColumn("is_weekend", 
        when(dayofweek(col("date_occured")).isin([1, 7]), True).otherwise(False)
    )

# Add season
df = df.withColumn("season",
    when(col("occurrence_month").isin([12, 1, 2]), "Winter")
    .when(col("occurrence_month").isin([3, 4, 5]), "Spring")
    .when(col("occurrence_month").isin([6, 7, 8]), "Summer")
    .otherwise("Fall")
)

df = df.withColumn("indexed_at", current_timestamp())

df.show(5)

df.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "gold-time-dimensions") \
    .mode("overwrite") \
    .save()

spark.stop()
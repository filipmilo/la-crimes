import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    when, datediff, lag, desc, asc, round as spark_round,
    dense_rank, percent_rank, ntile, coalesce, lit
)
from pyspark.sql.window import Window

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("Business Metrics") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

df = spark.read.parquet("s3a://la-crimes-data-lake/silver/cleaned.parquet")

# Calculate case resolution rates by area
resolution_stats = df.groupBy("area_id", "area_name") \
    .agg(
        count("*").alias("total_cases"),
        spark_sum(when(col("status_description").rlike("(?i)(closed|cleared|solved)"), 1).otherwise(0)).alias("resolved_cases")
    ) \
    .withColumn("resolution_rate", 
        spark_round(col("resolved_cases") / col("total_cases") * 100, 2)
    ) \
    .withColumn("area_performance_tier",
        when(col("resolution_rate") >= 75, "High Performance")
        .when(col("resolution_rate") >= 50, "Medium Performance")
        .otherwise("Low Performance")
    )

# Join resolution stats back to main dataframe
df = df.join(resolution_stats.select("area_id", "resolution_rate", "area_performance_tier"), "area_id", "left")

# Create crime density metrics
area_crime_density = df.groupBy("area_id") \
    .agg(count("*").alias("crime_count")) \
    .withColumn("crime_density_percentile",
        percent_rank().over(Window.orderBy("crime_count")) * 100
    ) \
    .withColumn("crime_density_category",
        when(col("crime_density_percentile") >= 90, "Very High Crime")
        .when(col("crime_density_percentile") >= 75, "High Crime")
        .when(col("crime_density_percentile") >= 50, "Medium Crime")
        .when(col("crime_density_percentile") >= 25, "Low Crime")
        .otherwise("Very Low Crime")
    )

# Join crime density back to main dataframe
df = df.join(area_crime_density.select("area_id", "crime_density_category", "crime_density_percentile"), "area_id", "left")

# Calculate reporting lag (days between occurrence and reporting)
df = df.withColumn("reporting_lag_days",
    when(
        col("date_reported").isNotNull() & col("date_occured").isNotNull(),
        datediff(col("date_reported"), col("date_occured"))
    ).otherwise(None)
)

# Categorize reporting lag
df = df.withColumn("reporting_speed",
    when(col("reporting_lag_days") == 0, "Same Day")
    .when(col("reporting_lag_days") <= 1, "Next Day")
    .when(col("reporting_lag_days") <= 7, "Within Week")
    .when(col("reporting_lag_days") <= 30, "Within Month")
    .when(col("reporting_lag_days") > 30, "Delayed")
    .otherwise("Unknown")
)

# Create temporal crime patterns
temporal_window = Window.partitionBy("area_id").orderBy("date_occured")

# Calculate crime trends (year-over-year change)
yearly_stats = df.groupBy("area_id", "occurrence_year") \
    .agg(count("*").alias("yearly_crime_count")) \
    .withColumn("prev_year_count",
        lag("yearly_crime_count", 1).over(
            Window.partitionBy("area_id").orderBy("occurrence_year")
        )
    ) \
    .withColumn("yoy_crime_change_pct",
        when(col("prev_year_count").isNotNull(),
            spark_round(
                (col("yearly_crime_count") - col("prev_year_count")) / col("prev_year_count") * 100, 2
            )
        ).otherwise(None)
    ) \
    .withColumn("crime_trend",
        when(col("yoy_crime_change_pct") > 10, "Increasing")
        .when(col("yoy_crime_change_pct") < -10, "Decreasing")
        .when(col("yoy_crime_change_pct").isNotNull(), "Stable")
        .otherwise("Unknown")
    )

# Join yearly trends back to main dataframe
df = df.join(
    yearly_stats.select("area_id", "occurrence_year", "yoy_crime_change_pct", "crime_trend"),
    ["area_id", "occurrence_year"], 
    "left"
)

# Create risk scores based on multiple factors
df = df.withColumn("area_risk_score",
    spark_round(
        (
            # Crime density factor (40% weight)
            (col("crime_density_percentile") / 100 * 0.4) +
            # Resolution rate factor (30% weight, inverted)
            ((100 - coalesce(col("resolution_rate"), lit(50))) / 100 * 0.3) +
            # Trend factor (30% weight)
            (when(col("crime_trend") == "Increasing", 0.3)
             .when(col("crime_trend") == "Decreasing", 0.1)
             .otherwise(0.2))
        ) * 100, 1
    )
)

# Categorize risk levels
df = df.withColumn("area_risk_level",
    when(col("area_risk_score") >= 75, "High Risk")
    .when(col("area_risk_score") >= 50, "Medium Risk")
    .when(col("area_risk_score") >= 25, "Low Risk")
    .otherwise("Very Low Risk")
)

df.show(5)

df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet("s3a://la-crimes-data-lake/gold/business_metrics.parquet")
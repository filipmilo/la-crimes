import sys
sys.path.insert(0, '/opt/airflow/dags')

from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    when, datediff, lag, desc, asc, round as spark_round,
    dense_rank, percent_rank, ntile, coalesce, lit, current_timestamp
)
from pyspark.sql.window import Window
from spark_config import create_spark_session_with_mongo, S3_BUCKET

spark = create_spark_session_with_mongo("Business Metrics")

df = spark.read.parquet(f"s3a://{S3_BUCKET}/silver/cleaned.parquet")

resolution_stats = df.groupBy("area_id") \
    .agg(
        count("*").alias("total_cases"),
        spark_sum(when(col("status_resolution") == "Closed", 1).otherwise(0)).alias("resolved_cases")
    ) \
    .withColumn("resolution_rate", 
        spark_round(col("resolved_cases") / col("total_cases") * 100, 2)
    ) \
    .withColumn("area_performance_tier",
        when(col("resolution_rate") >= 75, "High Performance")
        .when(col("resolution_rate") >= 50, "Medium Performance")
        .otherwise("Low Performance")
    )

df = df.join(resolution_stats.select("area_id", "resolution_rate", "area_performance_tier"), "area_id", "left")

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

df = df.join(area_crime_density.select("area_id", "crime_density_category", "crime_density_percentile"), "area_id", "left")

df = df.withColumn("reporting_lag_days",
    when(
        col("date_reported").isNotNull() & col("date_occured").isNotNull(),
        datediff(col("date_reported"), col("date_occured"))
    ).otherwise(None)
)

df = df.withColumn("reporting_speed",
    when(col("reporting_lag_days") == 0, "Same Day")
    .when(col("reporting_lag_days") <= 1, "Next Day")
    .when(col("reporting_lag_days") <= 7, "Within Week")
    .when(col("reporting_lag_days") <= 30, "Within Month")
    .when(col("reporting_lag_days") > 30, "Delayed")
    .otherwise("Unknown")
)

temporal_window = Window.partitionBy("area_id").orderBy("date_occured")

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

df = df.join(
    yearly_stats.select("area_id", "occurrence_year", "yoy_crime_change_pct", "crime_trend", "yearly_crime_count"),
    ["area_id", "occurrence_year"], 
    "left"
)

df = df.withColumn("area_risk_score",
    spark_round(
        (
            (col("crime_density_percentile") / 100 * 0.4) +
            ((100 - coalesce(col("resolution_rate"), lit(50))) / 100 * 0.3) +
            (when(col("crime_trend") == "Increasing", 0.3)
             .when(col("crime_trend") == "Decreasing", 0.1)
             .otherwise(0.2))
        ) * 100, 1
    )
)

df = df.withColumn("area_risk_level",
    when(col("area_risk_score") >= 75, "High Risk")
    .when(col("area_risk_score") >= 50, "Medium Risk")
    .when(col("area_risk_score") >= 25, "Low Risk")
    .otherwise("Very Low Risk")
)

df = df.withColumn("indexed_at", current_timestamp())

df.show(5)

df.write \
    .format("mongodb") \
    .option("collection", "gold_business_metrics") \
    .mode("overwrite") \
    .save()

df.select(
    "area_id", "area_name", "occurrence_year",
    "yearly_crime_count", "yoy_crime_change_pct", "crime_trend",
    "area_risk_score", "area_risk_level"
) \
    .dropDuplicates(["area_id", "occurrence_year"]) \
    .write.mode("overwrite") \
    .parquet(f"s3a://{S3_BUCKET}/silver/business_metrics/yoy_stats.parquet")

df.select(
    "area_id", "area_name",
    "resolution_rate", "area_performance_tier",
    "crime_density_percentile", "crime_density_category"
) \
    .dropDuplicates(["area_id"]) \
    .write.mode("overwrite") \
    .parquet(f"s3a://{S3_BUCKET}/silver/business_metrics/area_summary.parquet")

df.write.mode("overwrite") \
    .parquet(f"s3a://{S3_BUCKET}/silver/business_metrics/enriched.parquet")

spark.stop()

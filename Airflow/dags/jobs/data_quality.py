import sys
sys.path.insert(0, '/opt/airflow/dags')

from pyspark.sql.functions import (
    col, when, isnan, isnull, regexp_replace, upper, trim,
    coalesce, lit, length, size, split
)
from pyspark.sql.types import IntegerType
from spark_config import create_spark_session, S3_BUCKET

spark = create_spark_session("Data Quality Improvements")

df = spark.read.parquet(f"s3a://{S3_BUCKET}/silver/cleaned.parquet")

# Handle missing/invalid victim ages
df = df.withColumn("victim_age_clean",
    when(
        (col("victim_age").isNull()) |
        (col("victim_age") < 0) |
        (col("victim_age") > 120) |
        (isnan(col("victim_age")))
    , None)
    .otherwise(col("victim_age").cast(IntegerType()))
)

# Create age groups for better analysis
df = df.withColumn("victim_age_group",
    when(col("victim_age_clean").isNull(), "Unknown")
    .when(col("victim_age_clean") < 18, "Minor (0-17)")
    .when(col("victim_age_clean") < 25, "Young Adult (18-24)")
    .when(col("victim_age_clean") < 35, "Adult (25-34)")
    .when(col("victim_age_clean") < 50, "Middle Age (35-49)")
    .when(col("victim_age_clean") < 65, "Older Adult (50-64)")
    .otherwise("Senior (65+)")
)

# Standardize address formats
df = df.withColumn("street_address_standardized",
    when(col("street_address").isNotNull(),
        upper(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(col("street_address"), "\\s+", " "),
                        "^\\s+|\\s+$", ""
                    ),
                    "\\bST\\b|\\bSTREET\\b", "STREET"
                ),
                "\\bAVE\\b|\\bAVENUE\\b", "AVENUE"
            )
        )
    ).otherwise(None)
)

# Create data quality flags for incomplete records
df = df \
    .withColumn("has_victim_info",
        when(
            col("victim_age_clean").isNotNull() &
            col("victim_sex").isNotNull() &
            col("victim_descent").isNotNull()
        , True).otherwise(False)
    ) \
    .withColumn("has_location_info",
        when(
            col("lat").isNotNull() &
            col("lon").isNotNull() &
            col("street_address").isNotNull()
        , True).otherwise(False)
    ) \
    .withColumn("has_weapon_info",
        when(col("weapon_description").isNotNull(), True).otherwise(False)
    ) \
    .withColumn("has_premise_info",
        when(col("premise_description").isNotNull(), True).otherwise(False)
    )

# Calculate data completeness score (0-1 scale)
df = df.withColumn("data_completeness_score",
    (
        col("has_victim_info").cast("int") +
        col("has_location_info").cast("int") +
        col("has_weapon_info").cast("int") +
        col("has_premise_info").cast("int")
    ) / 4.0
)

# Create overall data quality flag
df = df.withColumn("data_quality_flag",
    when(col("data_completeness_score") >= 0.75, "High Quality")
    .when(col("data_completeness_score") >= 0.5, "Medium Quality")
    .when(col("data_completeness_score") >= 0.25, "Low Quality")
    .otherwise("Poor Quality")
)

# Flag potential data entry errors
df = df.withColumn("has_data_issues",
    when(
        (col("date_occured") > col("date_reported")) |  # Crime occurred after reported
        (col("victim_age") < 0) |  # Negative age
        (col("victim_age") > 120) |  # Unrealistic age
        (col("lat") == 0) & (col("lon") == 0)  # Invalid coordinates
    , True).otherwise(False)
)

df.show(5)

df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(f"s3a://{S3_BUCKET}/gold/data_quality_enhanced.parquet")
import sys
sys.path.insert(0, '/opt/airflow/dags')

from pyspark.sql.functions import (
    col, when, isnan, isnull, sqrt, pow, lit, count,
    round as spark_round, regexp_replace, upper, trim, current_timestamp, struct
)
from spark_config import create_spark_session_with_mongo, S3_BUCKET

spark = create_spark_session_with_mongo("Geographic Enrichment")

df = spark.read.parquet(f"s3a://{S3_BUCKET}/silver/cleaned.parquet")

# LA City Center coordinates (City Hall)
LA_CENTER_LAT = 34.0522
LA_CENTER_LON = -118.2437

# Validate lat/lon coordinates
df = df.withColumn("has_valid_coordinates",
    when(
        (col("lat").isNotNull()) & 
        (col("lon").isNotNull()) & 
        (~isnan(col("lat"))) & 
        (~isnan(col("lon"))) &
        (col("lat") != 0) & 
        (col("lon") != 0) &
        (col("lat").between(33.7, 34.4)) &  # LA County bounds
        (col("lon").between(-118.9, -117.6))
    , True).otherwise(False)
)

# Calculate distance from city center (in kilometers)
df = df.withColumn("distance_from_center_km",
    when(col("has_valid_coordinates"),
        spark_round(
            sqrt(
                pow((col("lat") - lit(LA_CENTER_LAT)) * 111, 2) +
                pow((col("lon") - lit(LA_CENTER_LON)) * 111 * 0.8, 2)  # Adjust for longitude
            ), 2
        )
    ).otherwise(None)
)

# Create geographic zones based on distance from center
df = df.withColumn("geographic_zone",
    when(col("distance_from_center_km") <= 8, "Downtown Core")
    .when(col("distance_from_center_km") <= 24, "Inner City")
    .when(col("distance_from_center_km") <= 48, "Greater LA")
    .when(col("distance_from_center_km") > 48, "Outer Areas")
    .otherwise("Unknown")
)

# Standardize street addresses
df = df.withColumn("street_address_clean",
    when(col("street_address").isNotNull(),
        upper(
            regexp_replace(
                regexp_replace(
                    regexp_replace(col("street_address"), "\\s+", " "),
                    "^\\s+|\\s+$", ""
                ),
                "\\bST\\b|\\bSTREET\\b", "STREET"
            )
        )
    ).otherwise(None)
)

# Create area density categories based on area_id
# Calculate crime count per area using DataFrame API
area_density = df.filter(col("area_id").isNotNull()) \
    .groupBy("area_id") \
    .agg(count("*").alias("crime_count"))

# Create density categories
area_density = area_density.withColumn("area_density_category",
    when(col("crime_count") >= 10000, "Very High Density")
    .when(col("crime_count") >= 5000, "High Density")
    .when(col("crime_count") >= 2000, "Medium Density")
    .when(col("crime_count") >= 500, "Low Density")
    .otherwise("Very Low Density")
)

# Join density information back to main dataframe
df = df.join(area_density.select("area_id", "area_density_category"), "area_id", "left")

# Create geo_point structure for Elasticsearch
df = df.withColumn("location",
    when(col("has_valid_coordinates"),
        struct(col("lat"), col("lon"))
    ).otherwise(None)
)

df = df.withColumn("indexed_at", current_timestamp())

df.show(5)

df.write \
    .format("mongodb") \
    .option("collection", "gold_geographic_enrichment") \
    .mode("overwrite") \
    .save()

spark.stop()
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, isnan, isnull, sqrt, pow, lit, 
    round as spark_round, regexp_replace, upper, trim
)

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("Geographic Enrichment") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

df = spark.read.parquet("s3a://la-crimes-data-lake/silver/cleaned.parquet")

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
area_density = spark.sql("""
    SELECT area_id, COUNT(*) as crime_count
    FROM temp_df 
    WHERE area_id IS NOT NULL
    GROUP BY area_id
""")

# Register temp view for the main dataframe
df.createOrReplaceTempView("temp_df")

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

df.show(5)

df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet("s3a://la-crimes-data-lake/gold/geo_enriched.parquet")
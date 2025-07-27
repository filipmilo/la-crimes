import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper, regexp_replace

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("Crime Categorization") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

df = spark.read.parquet("s3a://la-crimes-data-lake/silver/cleaned.parquet")

# Group crime descriptions into major categories
df = df.withColumn("crime_category",
    when(col("crime_description").rlike("(?i)(murder|homicide|manslaughter|kill)"), "Violent - Homicide")
    .when(col("crime_description").rlike("(?i)(rape|sexual|molest|sodomy)"), "Violent - Sexual")
    .when(col("crime_description").rlike("(?i)(robbery|armed|gun|weapon)"), "Violent - Robbery")
    .when(col("crime_description").rlike("(?i)(assault|battery|fight|attack)"), "Violent - Assault")
    .when(col("crime_description").rlike("(?i)(kidnap|abduct)"), "Violent - Kidnapping")
    .when(col("crime_description").rlike("(?i)(burglary|breaking|enter)"), "Property - Burglary")
    .when(col("crime_description").rlike("(?i)(theft|steal|larceny|shoplifting)"), "Property - Theft")
    .when(col("crime_description").rlike("(?i)(vehicle|auto|car|motorcycle).*theft"), "Property - Vehicle Theft")
    .when(col("crime_description").rlike("(?i)(vandalism|damage|destroy|graffiti)"), "Property - Vandalism")
    .when(col("crime_description").rlike("(?i)(arson|fire|burn)"), "Property - Arson")
    .when(col("crime_description").rlike("(?i)(drug|narcotic|marijuana|cocaine|heroin)"), "Drug")
    .when(col("crime_description").rlike("(?i)(fraud|embezzle|forgery|counterfeit)"), "White Collar")
    .when(col("crime_description").rlike("(?i)(domestic|intimate)"), "Domestic Violence")
    .when(col("crime_description").rlike("(?i)(disturbing|peace|noise|trespass)"), "Public Order")
    .when(col("crime_description").rlike("(?i)(drunk|alcohol|dui|dwi)"), "Alcohol Related")
    .otherwise("Other")
)

# Create severity levels based on crime types
df = df.withColumn("crime_severity",
    when(col("crime_category").rlike("(?i)homicide"), "Critical")
    .when(col("crime_category").rlike("(?i)(sexual|kidnapping|robbery)"), "High")
    .when(col("crime_category").rlike("(?i)(assault|burglary|vehicle theft|arson)"), "Medium")
    .when(col("crime_category").rlike("(?i)(theft|vandalism|drug|fraud)"), "Low")
    .otherwise("Minor")
)

# Flag domestic violence cases
df = df.withColumn("is_domestic_violence",
    when(
        col("crime_description").rlike("(?i)(domestic|intimate|spouse|partner)") |
        col("premise_description").rlike("(?i)(residence|home|house|apartment)")
    , True).otherwise(False)
)

# Create violent vs non-violent flag
df = df.withColumn("is_violent_crime",
    when(col("crime_category").rlike("(?i)violent"), True).otherwise(False)
)

# Create property crime flag
df = df.withColumn("is_property_crime",
    when(col("crime_category").rlike("(?i)property"), True).otherwise(False)
)

df.show(5)

df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet("s3a://la-crimes-data-lake/gold/crime_categorized.parquet")
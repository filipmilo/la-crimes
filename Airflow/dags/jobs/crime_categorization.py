import sys
sys.path.insert(0, '/opt/airflow/dags')

from pyspark.sql.functions import col, when, upper, regexp_replace, current_timestamp
from spark_config import create_spark_session_with_mongo, S3_BUCKET

spark = create_spark_session_with_mongo("Crime Categorization")

df = spark.read.parquet(f"s3a://{S3_BUCKET}/silver/cleaned.parquet")

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

df = df.withColumn("indexed_at", current_timestamp())

df.show(5)

df.write \
    .format("mongodb") \
    .option("collection", "gold_crime_categorization") \
    .mode("overwrite") \
    .save()

spark.stop()
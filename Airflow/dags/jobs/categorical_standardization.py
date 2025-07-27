import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper, trim

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("Categorical Standardization") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

df = spark.read.parquet("s3a://la-crimes-data-lake/silver/cleaned.parquet")

# Standardize victim sex values
df = df.withColumn("victim_sex_clean", 
    when(upper(trim(col("victim_sex"))).isin(["M", "MALE"]), "Male")
    .when(upper(trim(col("victim_sex"))).isin(["F", "FEMALE"]), "Female")
    .when(upper(trim(col("victim_sex"))).isin(["X", "H"]), "Other")
    .otherwise("Unknown")
)

# Clean and categorize victim descent codes
df = df.withColumn("victim_descent_clean",
    when(col("victim_descent") == "A", "Other Asian")
    .when(col("victim_descent") == "B", "Black")
    .when(col("victim_descent") == "C", "Chinese")
    .when(col("victim_descent") == "F", "Filipino")
    .when(col("victim_descent") == "G", "Guamanian")
    .when(col("victim_descent") == "H", "Hispanic/Latino")
    .when(col("victim_descent") == "I", "American Indian")
    .when(col("victim_descent") == "J", "Japanese")
    .when(col("victim_descent") == "K", "Korean")
    .when(col("victim_descent") == "L", "Laotian")
    .when(col("victim_descent") == "O", "Other")
    .when(col("victim_descent") == "P", "Pacific Islander")
    .when(col("victim_descent") == "S", "Samoan")
    .when(col("victim_descent") == "U", "Hawaiian")
    .when(col("victim_descent") == "V", "Vietnamese")
    .when(col("victim_descent") == "W", "White")
    .when(col("victim_descent") == "X", "Unknown")
    .when(col("victim_descent") == "Z", "Asian Indian")
    .otherwise("Unknown")
)

# Group weapon descriptions into broader categories
df = df.withColumn("weapon_category",
    when(col("weapon_description").rlike("(?i)(gun|pistol|rifle|firearm|revolver|shotgun)"), "Firearm")
    .when(col("weapon_description").rlike("(?i)(knife|blade|machete|sword|razor)"), "Knife/Blade")
    .when(col("weapon_description").rlike("(?i)(bat|club|hammer|pipe|stick|rock|bottle)"), "Blunt Object")
    .when(col("weapon_description").rlike("(?i)(hands|fist|feet|foot|personal)"), "Personal Weapons")
    .when(col("weapon_description").rlike("(?i)(vehicle|car|auto)"), "Vehicle")
    .when(col("weapon_description").rlike("(?i)(fire|burn|flame)"), "Fire/Incendiary")
    .when(col("weapon_description").rlike("(?i)(drug|poison|chemical)"), "Chemical/Poison")
    .when(col("weapon_description").isNull(), "Unknown")
    .otherwise("Other")
)

df.show(5)

df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet("s3a://la-crimes-data-lake/gold/categorical_standardized.parquet")

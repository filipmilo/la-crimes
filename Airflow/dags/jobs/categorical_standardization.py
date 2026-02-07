import sys
sys.path.insert(0, '/opt/airflow/dags')

from pyspark.sql.functions import col, when, upper, trim, current_timestamp
from spark_config import create_spark_session_with_es, S3_BUCKET

spark = create_spark_session_with_es("Categorical Standardization")

df = spark.read.parquet(f"s3a://{S3_BUCKET}/silver/cleaned.parquet")

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

df = df.withColumn("indexed_at", current_timestamp())

df.show(5)

df.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "gold-categorical-standardization") \
    .mode("overwrite") \
    .save()

spark.stop()

from pyspark.sql.functions import col, avg, lower, translate
from spark_utils import get_spark_session

spark = get_spark_session("Silver_Geolocation")
print("--- Starting Silver Pipeline: Geolocation ---")

df_bronze = spark.read.format("delta").load("s3a://olist-data/bronze/olist_geolocation")

# TRANSFORM & CLEAN: Remove the duplicates by grouping by Zip Code
df_normalized = df_bronze \
    .withColumn("geolocation_city", lower(col("geolocation_city"))) \
    .withColumn("geolocation_city", translate(col("geolocation_city"), "áàâãäéèêëíìîïóòôõöúùûüç", "aaaaaeeeeiiiiooooouuuuc"))

df_clean = df_normalized.groupBy("geolocation_zip_code_prefix", "geolocation_city", "geolocation_state").agg(
    avg("geolocation_lat").alias("lat"),
    avg("geolocation_lng").alias("lng")
)

# DQ CHECK: Ensure coordinates are valid (Latitude -90 to 90, Longitude -180 to 180)
invalid_coords = df_clean.filter(
    (col("lat") < -90) | (col("lat") > 90) | 
    (col("lng") < -180) | (col("lng") > 180)
).count()

if invalid_coords > 0:
    raise Exception(f"DQ L2 FAIL: Found {invalid_coords} zip codes with impossible GPS coordinates!")

# DQ CHECK 2: Ensure uniqueness of zip code
total_rows = df_clean.count()
unique_zips = df_clean.select("geolocation_zip_code_prefix").distinct().count()

if total_rows != unique_zips:
    print(f"DQ L2 WARNING: Found {total_rows - unique_zips} zip codes that still span across different city/state borders.")

# df_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://olist-data/silver/geolocation")
df_clean.write.format("delta").mode("overwrite").save("s3a://olist-data/silver/geolocation")
print("Successfully wrote to Silver")
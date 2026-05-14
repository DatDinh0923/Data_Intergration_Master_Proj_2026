from pyspark.sql.functions import col, avg, lower, translate, max as spark_max
from delta.tables import DeltaTable
from spark_utils import get_spark_session
import sys

spark = get_spark_session("Silver_Geolocation")
print("--- Starting Silver Pipeline: Geolocation (Incremental, Affected-Keys) ---")

BRONZE_PATH = "s3a://olist-data/bronze/olist_geolocation"
SILVER_PATH = "s3a://olist-data/silver/geolocation"

df_bronze = spark.read.format("delta").load(BRONZE_PATH)

# --- 1. INCREMENTAL WATERMARK ---
silver_exists = DeltaTable.isDeltaTable(spark, SILVER_PATH)
max_ingested = None
if silver_exists:
    silver_cols = spark.read.format("delta").load(SILVER_PATH).columns
    if "_ingested_at" in silver_cols:
        max_ingested = spark.read.format("delta").load(SILVER_PATH) \
            .selectExpr("max(_ingested_at)").collect()[0][0]
    else:
        # Legacy Silver (old overwrite-only version) had no _ingested_at after groupBy.
        # Force a rebuild so downstream Gold can detect future geo changes.
        print("Legacy Silver detected (no _ingested_at). Forcing initial rebuild...")
        silver_exists = False

# --- 2. NORMALIZE BRONZE (full — needed for re-aggregating affected zips) ---
df_normalized = df_bronze \
    .withColumn("geolocation_city", lower(col("geolocation_city"))) \
    .withColumn("geolocation_city", translate(col("geolocation_city"), "áàâãäéèêëíìîïóòôõöúùûüç", "aaaaaeeeeiiiiooooouuuuc"))

# --- 3. AFFECTED ZIPS ---
if max_ingested is not None:
    print(f"Last Silver update: {max_ingested}. Scanning Bronze for affected zip codes...")
    affected_zips = df_normalized.filter(col("_ingested_at") > max_ingested) \
        .select("geolocation_zip_code_prefix").distinct()

    if affected_zips.rdd.isEmpty():
        print("No affected zips. Pipeline finished.")
        sys.exit(0)

    n_affected = affected_zips.count()
    print(f"Found {n_affected} affected zip(s). Re-aggregating only those...")

    df_scope = df_normalized.join(affected_zips, on="geolocation_zip_code_prefix", how="inner")
else:
    print("Initial build: aggregating ALL zips.")
    df_scope = df_normalized

# --- 4. RE-AGGREGATE (full history per affected zip) ---
df_clean = df_scope.groupBy(
    "geolocation_zip_code_prefix", "geolocation_city", "geolocation_state"
).agg(
    avg("geolocation_lat").alias("lat"),
    avg("geolocation_lng").alias("lng"),
    spark_max("_ingested_at").alias("_ingested_at"),
)

# --- 5. DQ CHECKS ---
invalid_coords = df_clean.filter(
    (col("lat") < -90) | (col("lat") > 90) |
    (col("lng") < -180) | (col("lng") > 180)
).count()
if invalid_coords > 0:
    raise Exception(f"DQ L2 FAIL: Found {invalid_coords} zip codes with impossible GPS coordinates!")

total_rows = df_clean.count()
unique_zips = df_clean.select("geolocation_zip_code_prefix").distinct().count()
if total_rows != unique_zips:
    print(f"DQ L2 WARNING: Found {total_rows - unique_zips} zip codes that still span across different city/state borders.")

# --- 6. MERGE OR INITIAL OVERWRITE ---
if silver_exists:
    print("Target exists. Performing MERGE on (zip_code_prefix, city, state)...")
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)
    merge_condition = (
        "target.geolocation_zip_code_prefix = source.geolocation_zip_code_prefix AND "
        "target.geolocation_city = source.geolocation_city AND "
        "target.geolocation_state = source.geolocation_state"
    )
    silver_table.alias("target").merge(
        df_clean.alias("source"), merge_condition,
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    print("Target does not exist. Performing initial OVERWRITE...")
    df_clean.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(SILVER_PATH)

print("Successfully synced Geolocation to Silver.")

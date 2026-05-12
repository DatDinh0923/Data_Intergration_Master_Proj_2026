from pyspark.sql.functions import col
from delta.tables import DeltaTable
from spark_utils import get_spark_session
import sys

spark = get_spark_session("Silver_Reviews")
BRONZE_PATH = "s3a://olist-data/bronze/olist_reviews"
SILVER_PATH = "s3a://olist-data/silver/reviews"

df_bronze = spark.read.format("delta").load(BRONZE_PATH)

# --- 1. INCREMENTAL WATERMARK ---
if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    max_ingested = spark.read.format("delta").load(SILVER_PATH).selectExpr("max(_ingested_at)").collect()[0][0]
    if max_ingested:
        df_bronze = df_bronze.filter(col("_ingested_at") > max_ingested)

if df_bronze.count() == 0:
    print("No new data to process. Pipeline finished.")
    sys.exit(0)

# --- 2. TRANSFORM & CLEAN ---
# FIXED: Added lineage columns
df_clean = df_bronze.select(
    "review_id", "order_id", 
    col("review_score").cast("int"), 
    "_ingested_at", "_source_file"
).filter((col("review_score") >= 1) & (col("review_score") <= 5))

nulls = df_clean.filter(col("review_id").isNull() | col("order_id").isNull()).count()
if nulls > 0:
    raise Exception(f"DQ FAIL: Found {nulls} missing IDs in Reviews!")

# --- 3. MERGE OR OVERWRITE ---
if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    print("Target exists. Performing MERGE...")
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)
    silver_table.alias("target").merge(
        df_clean.alias("source"), "target.review_id = source.review_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    print("Target does not exist. Performing OVERWRITE...")
    df_clean.write.format("delta").mode("overwrite").save(SILVER_PATH)

print("Successfully synced Reviews to Silver.")
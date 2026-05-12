from pyspark.sql.functions import col
from delta.tables import DeltaTable
from spark_utils import get_spark_session
import sys

spark = get_spark_session("Silver_Zendesk")
BRONZE_PATH = "s3a://olist-data/bronze/helpdesk_tickets"
SILVER_PATH = "s3a://olist-data/silver/helpdesk"

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
# FIXED: Included _ingested_at and _source_file in the select
df_clean = df_bronze.select(
    "ticket_id", "email", "issue_type", 
    col("satisfaction_rating").cast("int"), 
    "_ingested_at", "_source_file"
)

invalid_ratings = df_clean.filter((col("satisfaction_rating") < 1) | (col("satisfaction_rating") > 5)).count()
if invalid_ratings > 0:
    raise Exception(f"DQ L2 FAIL: Found {invalid_ratings} support tickets with ratings outside 1-5!")

# --- 3. MERGE OR OVERWRITE ---
if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    print("Target exists. Performing MERGE...")
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)
    silver_table.alias("target").merge(
        df_clean.alias("source"), "target.ticket_id = source.ticket_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    print("Target does not exist. Performing OVERWRITE...")
    df_clean.write.format("delta").mode("overwrite").save(SILVER_PATH)

print("Successfully synced Helpdesk to Silver.")
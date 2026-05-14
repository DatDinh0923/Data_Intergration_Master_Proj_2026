from pyspark.sql.functions import col, datediff, to_date, lit, current_timestamp
from delta.tables import DeltaTable
from spark_utils import get_spark_session
import sys

spark = get_spark_session("Gold_Customer_RFM")
print("--- Starting Gold Layer: RFM (Incremental, derived from customer_360) ---")

C360_PATH = "s3a://olist-data/gold/customer_360"
RFM_PATH  = "s3a://olist-data/gold/segment_rfm"

df_c360 = spark.read.format("delta").load(C360_PATH)

# --- 1. WATERMARK against RFM's own _last_updated_at ---
rfm_exists = DeltaTable.isDeltaTable(spark, RFM_PATH)
rfm_max_ts = None
if rfm_exists:
    rfm_cols = spark.read.format("delta").load(RFM_PATH).columns
    if "_last_updated_at" in rfm_cols:
        rfm_max_ts = spark.read.format("delta").load(RFM_PATH) \
            .selectExpr("max(_last_updated_at)").collect()[0][0]
    else:
        print("Legacy RFM detected (no _last_updated_at). Forcing initial rebuild...")
        rfm_exists = False

# customer_360 carries its own _last_updated_at — only re-compute customers refreshed since last RFM build
if rfm_max_ts is not None:
    df_c360 = df_c360.filter(col("_last_updated_at") > rfm_max_ts)

if df_c360.rdd.isEmpty():
    print("No customers changed since last RFM build. Pipeline finished.")
    sys.exit(0)

print(f"Processing {df_c360.count()} affected customer(s) for RFM...")

# --- 2. DERIVE RFM ---
# R = recency_days (days between fixed reference date and last_purchase_date)
# F = total_orders          (already in customer_360)
# M = total_lifetime_value  (already in customer_360)
df_rfm_new = df_c360.withColumn(
    "recency_days",
    datediff(to_date(lit("2018-10-01")), col("last_purchase_date")),
).withColumn("_last_updated_at", current_timestamp())

# --- 3. MERGE OR INITIAL OVERWRITE ---
if rfm_exists:
    print("Target exists. Performing MERGE on customer_unique_id...")
    rfm_table = DeltaTable.forPath(spark, RFM_PATH)
    rfm_table.alias("target").merge(
        df_rfm_new.alias("source"),
        "target.customer_unique_id = source.customer_unique_id",
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    print("Target does not exist. Performing initial OVERWRITE...")
    df_rfm_new.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(RFM_PATH)

print("Success! RFM raw table synced.")

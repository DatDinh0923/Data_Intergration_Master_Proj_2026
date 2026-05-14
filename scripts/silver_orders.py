from pyspark.sql.functions import col, to_timestamp, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from spark_utils import get_spark_session
import sys

spark = get_spark_session("Silver_Orders")
print("--- Starting Silver Pipeline: Orders (Incremental, Status-Aware) ---")

BRONZE_PATH = "s3a://olist-data/bronze/olist_orders"
SILVER_PATH = "s3a://olist-data/silver/orders"

df_bronze = spark.read.format("delta").load(BRONZE_PATH)

# --- 1. SCHEMA GUARD ---
REQUIRED_COLUMNS = [
    "order_id", "customer_id", "order_status", "order_purchase_timestamp",
    "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date",
    "order_estimated_delivery_date",
]
missing_cols = [c for c in REQUIRED_COLUMNS if c not in df_bronze.columns]
if missing_cols:
    raise Exception(f"FATAL: Source missing mandatory columns: {missing_cols}. Halting pipeline!")

# --- 2. INCREMENTAL WATERMARK ---
silver_exists = DeltaTable.isDeltaTable(spark, SILVER_PATH)
max_ingested = None
if silver_exists:
    max_ingested = spark.read.format("delta").load(SILVER_PATH) \
        .selectExpr("max(_ingested_at)").collect()[0][0]

if max_ingested is not None:
    df_bronze = df_bronze.filter(col("_ingested_at") > max_ingested)

if df_bronze.rdd.isEmpty():
    print("No new data to process. Pipeline finished.")
    sys.exit(0)

# --- 3. KEEP ONLY LATEST BRONZE ROW PER order_id ---
# Bronze can contain multiple rows for the same order_id when the upstream system
# replays status updates. We only care about the most recent state per key.
w_latest = Window.partitionBy("order_id").orderBy(col("_ingested_at").desc())
df_latest = df_bronze.withColumn("_rn", row_number().over(w_latest)) \
                     .filter(col("_rn") == 1).drop("_rn")

# --- 4. TRANSFORM & CLEAN ---
df_cleaned = df_latest \
    .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"))) \
    .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date")))

null_orders = df_cleaned.filter(col("order_id").isNull()).count()
if null_orders > 0:
    raise Exception(f"DQ FAIL: Found {null_orders} null order_ids! Pipeline halted.")

final_columns = [
    "order_id", "customer_id", "order_status", "order_purchase_timestamp",
    "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date",
    "order_estimated_delivery_date", "_ingested_at", "_source_file",
]
df_source = df_cleaned.select(*final_columns)

# --- 5. MERGE OR INITIAL OVERWRITE (Status-Aware Whitelist) ---
# Silver only stores delivered orders. The MERGE must therefore:
#   - INSERT new orders whose latest status is 'delivered'
#   - UPDATE existing rows whose latest status is still 'delivered'
#   - DELETE existing rows whose latest status switched away from 'delivered'
#     (cancellation, return, etc. — keeps Silver consistent with the whitelist)
if silver_exists:
    print("Target exists. Performing status-aware MERGE on order_id...")
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)
    silver_table.alias("target").merge(
        df_source.alias("source"),
        "target.order_id = source.order_id",
    ).whenMatchedDelete(
        condition="source.order_status <> 'delivered'"
    ).whenMatchedUpdateAll(
        condition="source.order_status = 'delivered'"
    ).whenNotMatchedInsertAll(
        condition="source.order_status = 'delivered'"
    ).execute()
else:
    print("Target does not exist. Performing initial OVERWRITE...")
    df_source.filter(col("order_status") == "delivered") \
        .write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(SILVER_PATH)

print("Successfully synced Orders to Silver.")

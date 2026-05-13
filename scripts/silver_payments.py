from pyspark.sql.functions import col
from delta.tables import DeltaTable
from spark_utils import get_spark_session
import sys

spark = get_spark_session("Silver_Payments")
BRONZE_PATH = "s3a://olist-data/bronze/olist_payments"
SILVER_PATH = "s3a://olist-data/silver/payments"

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
REQUIRED_COLUMNS = ["order_id", "payment_type", "payment_value"]
missing_cols = [c for c in REQUIRED_COLUMNS if c not in df_bronze.columns]
if missing_cols:
    raise Exception(f"FATAL: Source missing mandatory columns: {missing_cols}. Halting pipeline!")

df_casted = df_bronze.withColumn("payment_value", col("payment_value").cast("double"))

# DQ Check: cast must not silently produce NULLs (would happen on non-numeric source values)
cast_failures = df_casted.filter(col("payment_value").isNull()).count()
if cast_failures > 0:
    raise Exception(f"DQ L2 FAIL: Found {cast_failures} rows where payment_value cast to NULL — bad source data!")

df_clean = df_casted.filter(col("payment_value") >= 0)

null_orders = df_clean.filter(col("order_id").isNull()).count()
if null_orders > 0:
    raise Exception(f"DQ L2 FAIL: Found {null_orders} payments missing an order_id!")

final_columns = [
    "order_id", "payment_sequential", "payment_type", 
    "payment_installments", "payment_value", "_ingested_at", "_source_file"
]
df_clean = df_clean.select(*final_columns)

# --- 3. MERGE OR OVERWRITE ---
if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    print("Target exists. Performing MERGE...")
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)
    merge_condition = "target.order_id = source.order_id AND target.payment_sequential = source.payment_sequential"
    
    silver_table.alias("target").merge(
        df_clean.alias("source"), merge_condition
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    print("Target does not exist. Performing OVERWRITE...")
    df_clean.write.format("delta").mode("overwrite").save(SILVER_PATH)

print("Successfully synced Payments to Silver.")
from pyspark.sql.functions import col
from delta.tables import DeltaTable
from spark_utils import get_spark_session
import sys

spark = get_spark_session("Silver_Items")
BRONZE_PATH = "s3a://olist-data/bronze/olist_items"
SILVER_PATH = "s3a://olist-data/silver/order_items"

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
# If the team returns missing these important columns, halt the pipeline.
REQUIRED_COLUMNS = ["order_id", "product_id", "price", "freight_value"]
missing_cols = [c for c in REQUIRED_COLUMNS if c not in df_bronze.columns]
if missing_cols:
    raise Exception(f"FATAL: Source missing mandatory columns: {missing_cols}. Halting pipeline!")

df_clean = df_bronze \
    .withColumn("price", col("price").cast("double")) \
    .withColumn("freight_value", col("freight_value").cast("double")) \
    .filter(col("price") >= 0)

# DQ Check: No missing foreign keys
null_foreign_keys = df_clean.filter(col("order_id").isNull() | col("product_id").isNull()).count()
if null_foreign_keys > 0:
    raise Exception(f"DQ FAIL: Found {null_foreign_keys} items missing order/product IDs!")

final_columns = [
    "order_id", "order_item_id", "product_id", "seller_id", 
    "shipping_limit_date", "price", "freight_value", "_ingested_at", "_source_file"
]
df_clean = df_clean.select(*final_columns)

# --- 3. MERGE OR OVERWRITE (Composite Key) ---
if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    print("Target exists. Performing MERGE...")
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)
    
    # Matching on BOTH order_id and order_item_id
    merge_condition = "target.order_id = source.order_id AND target.order_item_id = source.order_item_id"
    
    silver_table.alias("target").merge(
        df_clean.alias("source"), merge_condition
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    print("Target does not exist. Performing OVERWRITE...")
    df_clean.write.format("delta").mode("overwrite").save(SILVER_PATH)

print("Successfully synced Items to Silver.")
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.utils import AnalysisException
from spark_utils import get_spark_session

spark = get_spark_session("Silver_Orders")

# LOAD BRONZE DATA
df_orders_bronze = spark.read.format("delta").load("s3a://olist-data/bronze/olist_orders")

# If the team return missing these important columns then it will affect the GOLD, so halt here if possible.
REQUIRED_COLUMNS = [
    "order_id", "customer_id", "order_status", "order_purchase_timestamp", 
    "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", 
    "order_estimated_delivery_date"
]
missing_cols = [c for c in REQUIRED_COLUMNS if c not in df_orders_bronze.columns]
if missing_cols:
    raise Exception(f"FATAL: Source missing mandatory columns: {missing_cols}. Halting pipeline!")

# TRANSFORM & CLEAN
# Whitelist approach: Only keep successfully delivered orders
# delivered, shipped, canceled, unavailable, invoiced, processing, created, and approved
df_cleaned = df_orders_bronze.filter(col("order_status") == "delivered")
df_cleaned = df_cleaned.withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"))) \
                       .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date")))

# DQ CHECKS
print("Running DQ Checks olist_orders_dataset.csv ...")
# Check for null primary keys
print("DQ Check: If there is any NULL value in order_id ...")
null_orders = df_cleaned.filter(col("order_id").isNull()).count()
if null_orders > 0:
    raise Exception(f"DQ FAIL: Found {null_orders} null order_ids! Pipeline halted.")

print("DQ Checks Passed! Proceeding to write to Silver.")

final_columns = [
    "order_id", "customer_id", "order_status", "order_purchase_timestamp", 
    "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", 
    "order_estimated_delivery_date", "_ingested_at", "_source_file"
]

df_cleaned.select(*final_columns).write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("s3a://olist-data/silver/orders")

print("Successfully wrote Orders to Silver Delta Table.")
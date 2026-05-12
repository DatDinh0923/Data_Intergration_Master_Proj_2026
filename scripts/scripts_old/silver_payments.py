from pyspark.sql.functions import col
from spark_utils import get_spark_session

spark = get_spark_session("Silver_Payments")
df_bronze = spark.read.format("delta").load("s3a://olist-data/bronze/olist_payments")

REQUIRED_COLUMNS = ["order_id", "payment_type", "payment_value"]
missing_cols = [c for c in REQUIRED_COLUMNS if c not in df_bronze.columns]
if missing_cols:
    raise Exception(f"FATAL: Source missing mandatory columns: {missing_cols}. Halting pipeline!")

# TRANSFORM & CLEAN: Ensure payment_value is a float and positive
df_clean = df_bronze.withColumn("payment_value", col("payment_value").cast("double")) \
                    .filter(col("payment_value") >= 0)

# DQ CHECK: No null Order IDs
null_orders = df_clean.filter(col("order_id").isNull()).count()
if null_orders > 0:
    raise Exception(f"DQ L2 FAIL: Found {null_orders} payments missing an order_id!")

final_columns = [
    "order_id", "payment_sequential", "payment_type", 
    "payment_installments", "payment_value", "_ingested_at", "_source_file"
]

df_clean.select(*final_columns).write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save("s3a://olist-data/silver/payments")

print("Successfully wrote to Silver")
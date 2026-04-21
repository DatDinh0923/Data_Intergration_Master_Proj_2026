from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.utils import AnalysisException
from spark_utils import get_spark_session

spark = get_spark_session("Silver_Orders")

# 1. LOAD BRONZE DATA
df_orders_bronze = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("s3a://olist-data/bronze/olist_orders_dataset.csv")

# 2. TRANSFORM & CLEAN
# Whitelist approach: Only keep successfully delivered orders
# delivered, shipped, canceled, unavailable, invoiced, processing, created, and approved
df_cleaned = df_orders_bronze.filter(col("order_status") == "delivered")

# Convert string dates to actual PySpark Timestamps
df_cleaned = df_cleaned.withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"))) \
                       .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date")))

# 3. DQ CHECKS
print("Running DQ Checks...")
# Check for null primary keys
print("DQ Check: If there is any NULL value in order_id ...")
null_orders = df_cleaned.filter(col("order_id").isNull()).count()
if null_orders > 0:
    raise Exception(f"DQ FAIL: Found {null_orders} null order_ids! Pipeline halted.")

print("DQ Checks Passed! Proceeding to write to Silver.")

# 4. WRITE TO SILVER (Delta format)
df_cleaned.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://olist-data/silver/orders")

print("Successfully wrote Orders to Silver Delta Table.")
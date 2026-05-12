from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException
from spark_utils import get_spark_session

spark = get_spark_session("Silver_Items")


# LOAD BRONZE
df_items_bronze = spark.read.format("delta").load("s3a://olist-data/bronze/olist_items")

# If the team return missing these important columns then it will affect the GOLD, so halt here if possible.
REQUIRED_COLUMNS = ["order_id", "product_id", "price", "freight_value"]
missing_cols = [c for c in REQUIRED_COLUMNS if c not in df_items_bronze.columns]
if missing_cols:
    raise Exception(f"FATAL: Source missing mandatory columns: {missing_cols}. Halting pipeline!")

# TRANSFORM & CLEAN
# Ensure price and freight are numbers, and filter out impossible negative values
df_items_clean = df_items_bronze \
    .withColumn("price", col("price").cast("double")) \
    .withColumn("freight_value", col("freight_value").cast("double")) \
    .filter(col("price") >= 0)

# DQ CHECKS
print("Running Automated DQ Checks olist_order_items_dataset.csv ...")
print("DQ Check: If there is any NULL value in neither order_id nor product_id ...")
null_foreign_keys = df_items_clean.filter(col("order_id").isNull() | col("product_id").isNull()).count()
if null_foreign_keys > 0:
    raise Exception(f"DQ FAIL: Found {null_foreign_keys} items missing order/product IDs!")
print("DQ Checks Passed! Proceeding to write to Silver.")


final_columns = [
    "order_id", "order_item_id", "product_id", "seller_id", 
    "shipping_limit_date", "price", "freight_value", "_ingested_at", "_source_file"
]

df_items_clean.select(*final_columns).write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save("s3a://olist-data/silver/order_items")

print("Successfully wrote to Silver.")
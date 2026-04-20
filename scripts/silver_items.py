from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException
from spark_utils import get_spark_session
# 1. LOAD BRONZE
df_items_bronze = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("s3a://olist-data/bronze/olist_order_items_dataset.csv")

# 2. TRANSFORM & CLEAN
# Ensure price and freight are numbers, and filter out impossible negative values
df_items_clean = df_items_bronze \
    .withColumn("price", col("price").cast("double")) \
    .withColumn("freight_value", col("freight_value").cast("double")) \
    .filter(col("price") >= 0)

# 3. DQ CHECKS
print("DQ Check: If there is any NULL value in neither order_id nor product_id ...")
null_foreign_keys = df_items_clean.filter(col("order_id").isNull() | col("product_id").isNull()).count()
if null_foreign_keys > 0:
    raise Exception(f"DQ FAIL: Found {null_foreign_keys} items missing order/product IDs!")
print("DQ Checks Passed! Proceeding to write to Silver.")

# 4. WRITE TO SILVER
df_items_clean.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://olist-data/silver/order_items")

print("Successfully wrote Order Items to Silver.")
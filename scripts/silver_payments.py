from pyspark.sql.functions import col
from spark_utils import get_spark_session

spark = get_spark_session("Silver_Payments")
print("--- Starting Silver Pipeline: Payments ---")

df_bronze = spark.read.format("delta").load("s3a://olist-data/bronze/olist_payments")

# 1. TRANSFORM & CLEAN: Ensure payment_value is a float and positive
df_clean = df_bronze.withColumn("payment_value", col("payment_value").cast("double")) \
                    .filter(col("payment_value") >= 0)

# 2. DQ CHECK: No null Order IDs
null_orders = df_clean.filter(col("order_id").isNull()).count()
if null_orders > 0:
    raise Exception(f"DQ L2 FAIL: Found {null_orders} payments missing an order_id!")

df_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://olist-data/silver/payments")
print("Success: Payments written to Silver.")
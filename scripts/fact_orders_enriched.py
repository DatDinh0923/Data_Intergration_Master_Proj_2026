from pyspark.sql.functions import col, sum as _sum, count, datediff, round as spark_round
from spark_utils import get_spark_session

spark = get_spark_session("Gold_Fact_Orders")
print("--- Starting Gold Layer: Fact Orders Enriched ---")

# LOAD SILVER DATA
df_orders = spark.read.format("delta").load("s3a://olist-data/silver/orders")
df_items = spark.read.format("delta").load("s3a://olist-data/silver/order_items")
df_reviews = spark.read.format("delta").load("s3a://olist-data/silver/reviews")
df_customers = spark.read.format("delta").load("s3a://olist-data/silver/customers")

# AGGREGATE ITEMS (To avoid duplicate rows when joining)
df_items_agg = df_items.groupBy("order_id").agg(
    spark_round(_sum("price"), 2).alias("total_revenue"),
    spark_round(_sum("freight_value"), 2).alias("total_freight"),
    count("product_id").alias("total_items")
)

# ENRICH ORDERS
# We calculate 'days_to_deliver' and 'delivery_delay_days'
df_fact_orders = df_orders \
    .join(df_items_agg, on="order_id", how="inner") \
    .join(df_reviews.select("order_id", "review_score"), on="order_id", how="left") \
    .join(df_customers.select("customer_id", "customer_city", "customer_state"), on="customer_id", how="left") \
    .withColumn("days_to_deliver", datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))) \
    .withColumn("delivery_delay_days", datediff(col("order_delivered_customer_date"), col("order_estimated_delivery_date")))

# DQ L3 CHECK: Integrity Check
null_purchases = df_fact_orders.filter(col("order_purchase_timestamp").isNull()).count()
if null_purchases > 0:
    raise Exception(f"DQ L3 FAIL: Found {null_purchases} orders missing a purchase date!")

df_fact_orders.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://olist-data/gold/fact_orders_enriched")
    # .option("overwriteSchema", "true") \

print("Success! Fact Orders Enriched saved.")
# df_fact_orders.select("order_id", "days_to_deliver", "delivery_delay_days", "review_score", "total_revenue").show(5)
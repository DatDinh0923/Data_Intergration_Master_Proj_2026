from pyspark.sql.functions import col, sum, countDistinct, max, round, avg, mode
from spark_utils import get_spark_session

spark = get_spark_session("Gold_CDP")
print("--- Starting Gold Layer: The Ultimate Customer 360 ---")

# 1. LOAD ALL 5 SILVER TABLES
df_customers = spark.read.format("delta").load("s3a://olist-data/silver/customers")
df_orders = spark.read.format("delta").load("s3a://olist-data/silver/orders")
df_items = spark.read.format("delta").load("s3a://olist-data/silver/order_items")
df_reviews = spark.read.format("delta").load("s3a://olist-data/silver/reviews")
df_products = spark.read.format("delta").load("s3a://olist-data/silver/products")

# 2. THE BIG JOIN 
# Chain the joins together carefully using LEFT JOINs where data might be missing
df_full = df_orders \
    .join(df_items, on="order_id", how="inner") \
    .join(df_customers, on="customer_id", how="inner") \
    .join(df_products, on="product_id", how="left") \
    .join(df_reviews, on="order_id", how="left")

# 3. AGGREGATE THE CDP
df_customer_360 = df_full.groupBy("customer_unique_id", "customer_city").agg(
    countDistinct("order_id").alias("total_orders"),
    round(sum(col("price") + col("freight_value")), 2).alias("total_lifetime_value"),
    max("order_purchase_timestamp").alias("last_purchase_date"),
    round(avg("review_score"), 1).alias("average_review_score"),
    mode("product_category_name").alias("favorite_category") 
)

# 4. WRITE TO GOLD
# df_customer_360.write.format("delta").mode("overwrite").save("s3a://olist-data/gold/customer_360")

df_customer_360.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("s3a://olist-data/gold/customer_360")

print("Success! Customer 360 Table is fully enriched.")


df_customer_360.orderBy(col("total_lifetime_value").desc()).show(5)
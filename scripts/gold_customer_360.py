from pyspark.sql.functions import col, sum as _sum, countDistinct, max as spark_max, round as spark_round, avg, mode, count
from spark_utils import get_spark_session

spark = get_spark_session("Gold_CDP")
print("--- Starting Gold Layer: The Ultimate Multi-Channel Customer 360 ---")


df_customers = spark.read.format("delta").load("s3a://olist-data/silver/customers")
df_orders = spark.read.format("delta").load("s3a://olist-data/silver/orders")
df_items = spark.read.format("delta").load("s3a://olist-data/silver/order_items")
df_reviews = spark.read.format("delta").load("s3a://olist-data/silver/reviews")
df_products = spark.read.format("delta").load("s3a://olist-data/silver/products")
df_crm = spark.read.format("delta").load("s3a://olist-data/silver/crm")
df_zendesk = spark.read.format("delta").load("s3a://olist-data/silver/zendesk")
df_geo = spark.read.format("delta").load("s3a://olist-data/silver/geolocation")


df_ecommerce_full = df_orders \
    .join(df_items, on="order_id", how="inner") \
    .join(df_customers, on="customer_id", how="inner") \
    .join(df_products, on="product_id", how="left") \
    .join(df_reviews, on="order_id", how="left")


df_ecommerce_agg = df_ecommerce_full.groupBy("customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state").agg(
    countDistinct("order_id").alias("total_orders"),
    spark_round(_sum(col("price") + col("freight_value")), 2).alias("total_lifetime_value"),
    spark_max("order_purchase_timestamp").alias("last_purchase_date"),
    spark_round(avg("review_score"), 1).alias("average_review_score"),
    mode("product_category_name").alias("favorite_category") 
)

# AGGREGATE SUPPORT DATA
df_support_agg = df_zendesk.groupBy("email").agg(
    count("ticket_id").alias("total_support_tickets"),
    avg("satisfaction_rating").alias("avg_support_rating")
)

# MULTI-CHANNEL CDP MERGE
df_customer_360 = df_ecommerce_agg \
    .join(df_crm, on="customer_unique_id", how="left") \
    .join(df_support_agg, on="email", how="left") \
    .join(df_geo, df_ecommerce_agg.customer_zip_code_prefix == df_geo.geolocation_zip_code_prefix, "left") \
    .fillna({"total_support_tickets": 0}) \
    .drop("geolocation_zip_code_prefix", "customer_zip_code_prefix") # Clean up duplicate join keys

# DQ L3 CHECK: Ensure we didn't lose customers
if df_customer_360.count() < df_crm.count():
    print("DQ L3 WARNING: Customer count dropped during 360 Merge!")

df_customer_360.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("s3a://olist-data/gold/customer_360")

print("Success! Multi-Channel Customer 360 Table is fully enriched.")

df_customer_360.select(
    "customer_unique_id", "email", "total_lifetime_value", 
    "favorite_category", "average_review_score", "total_support_tickets", "lat"
).orderBy(col("total_lifetime_value").desc()).show(5, truncate=False)
from pyspark.sql.functions import col, sum as _sum, countDistinct, max as spark_max, round as spark_round, mode, avg, expr, first, count
from spark_utils import get_spark_session

spark = get_spark_session("Gold_CDP_Full_Enrichment")
print("--- Starting Gold Layer: Customer 360 + CRM + Helpdesk ---")

# 1. LOAD SILVER TABLES
df_customers = spark.read.format("delta").load("s3a://olist-data/silver/customers")
df_orders = spark.read.format("delta").load("s3a://olist-data/silver/orders")
df_items = spark.read.format("delta").load("s3a://olist-data/silver/order_items")
df_products = spark.read.format("delta").load("s3a://olist-data/silver/products")
df_geo = spark.read.format("delta").load("s3a://olist-data/silver/geolocation")
df_crm = spark.read.format("delta").load("s3a://olist-data/silver/crm")
df_helpdesk = spark.read.format("delta").load("s3a://olist-data/silver/helpdesk")

# 2. PRE-PROCESS GEOLOCATION (Grain: 1 row per zip_code)
df_geo_clean = df_geo.groupBy("geolocation_zip_code_prefix").agg(
    avg("lat").alias("lat"),
    avg("lng").alias("lng"),
    first("geolocation_city").alias("geolocation_city"),
    first("geolocation_state").alias("geolocation_state")
)

# 3. PRE-PROCESS HELPDESK (Grain: 1 row per email)
# We aggregate metrics so we don't create duplicate rows for customers with multiple tickets
df_helpdesk_agg = df_helpdesk.groupBy("email").agg(
    count("ticket_id").alias("total_support_tickets"),
    spark_round(avg("satisfaction_rating"), 1).alias("avg_support_rating")
)

# 4. BUILD E-COMMERCE BASE
df_ecommerce_full = df_orders \
    .join(df_items, on="order_id", how="inner") \
    .join(df_customers, on="customer_id", how="inner") \
    .join(df_products, on="product_id", how="left")

# 5. AGGREGATE E-COMMERCE (Grain: 1 row per customer_unique_id)
df_ecommerce_agg = df_ecommerce_full.groupBy("customer_unique_id").agg(
    expr("max_by(customer_zip_code_prefix, order_purchase_timestamp)").alias("customer_zip_code_prefix"),
    expr("max_by(customer_city, order_purchase_timestamp)").alias("customer_city"),
    expr("max_by(customer_state, order_purchase_timestamp)").alias("customer_state"),
    countDistinct("order_id").alias("total_orders"),
    spark_round(_sum(col("price") + col("freight_value")), 2).alias("total_lifetime_value"),
    spark_max("order_purchase_timestamp").alias("last_purchase_date"),
    mode("product_category_name").alias("favorite_category") 
)

# 6. FINAL JOIN (CRM -> Helpdesk -> Geo)
# Join CRM first to get the email, then use that email to join Helpdesk data
df_customer_360 = df_ecommerce_agg \
    .join(df_crm, on="customer_unique_id", how="left") \
    .join(df_helpdesk_agg, on="email", how="left") \
    .join(df_geo_clean, df_ecommerce_agg.customer_zip_code_prefix == df_geo_clean.geolocation_zip_code_prefix, "left") \
    .fillna({"total_support_tickets": 0}) \
    .drop("geolocation_zip_code_prefix", "customer_zip_code_prefix")

# 7. DQ CHECK
actual_count = df_customer_360.count()
expected_count = df_ecommerce_agg.count()

if actual_count > expected_count:
    print(f"DQ WARNING: Row explosion detected! {actual_count} vs {expected_count}")

# 8. SAVE TO GOLD
df_customer_360.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("s3a://olist-data/gold/customer_360")

print(f"Success! Integrated Ecommerce, CRM, and Helpdesk data for {actual_count} unique customers.")
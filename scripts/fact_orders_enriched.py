from pyspark.sql.functions import col, sum as _sum, count
from spark_utils import get_spark_session

spark = get_spark_session("Gold_Fact_Orders")
print("--- Starting Gold Layer: Fact Orders Enriched ---")

# 1. Đọc dữ liệu Silver
df_orders = spark.read.format("delta").load("s3a://olist-data/silver/orders")
df_items = spark.read.format("delta").load("s3a://olist-data/silver/order_items")
df_reviews = spark.read.format("delta").load("s3a://olist-data/silver/reviews")
df_customers = spark.read.format("delta").load("s3a://olist-data/silver/customers")

# 2. Gom nhóm items để tính tổng tiền cho mỗi đơn hàng
df_items_agg = df_items.groupBy("order_id").agg(
    _sum("price").alias("total_revenue"),
    _sum("freight_value").alias("total_freight"),
    count("product_id").alias("total_items")
)

# 3. Join (Nối) tất cả lại với nhau
df_fact_orders = df_orders \
    .join(df_items_agg, on="order_id", how="inner") \
    .join(df_reviews.select("order_id", "review_score"), on="order_id", how="left") \
    .join(df_customers.select("customer_id", "customer_city", "customer_state"), on="customer_id", how="left")

# 4. Ghi ra tầng Gold
df_fact_orders.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("s3a://olist-data/gold/fact_orders_enriched")

print("Success! Fact Orders Enriched saved.")
df_fact_orders.select("order_id", "total_revenue", "total_items", "review_score", "customer_state").show(5)
from pyspark.sql.functions import col, sum, countDistinct, max, round
from spark_utils import get_spark_session
print("--- Starting Gold Layer: Customer 360 ---")

spark = get_spark_session("Gold_CDP")

# 1. LOAD SILVER DATA (No schemas needed, Delta remembers!)
df_customers = spark.read.format("delta").load("s3a://olist-data/silver/customers")
df_orders = spark.read.format("delta").load("s3a://olist-data/silver/orders")
df_items = spark.read.format("delta").load("s3a://olist-data/silver/order_items")

# 2. THE BIG JOIN (Fusing the data together)
# Join Orders and Items to figure out the cost of each order
df_order_spend = df_orders.join(df_items, on="order_id", how="inner")

# Join with Customers to attach the real human ID and their city
df_full_history = df_order_spend.join(df_customers, on="customer_id", how="inner")

# 3. AGGREGATE THE CDP (Squash it down to one row per human)
# Notice we group by customer_UNIQUE_id here!
df_customer_360 = df_full_history.groupBy("customer_unique_id", "customer_city").agg(
    
    # Frequency: Count how many unique orders they placed
    countDistinct("order_id").alias("total_orders"),
    
    # Monetary: Sum of all item prices + freight costs, rounded to 2 decimals
    round(sum(col("price") + col("freight_value")), 2).alias("total_lifetime_value"),
    
    # Recency: Find the timestamp of their most recent purchase
    max("order_purchase_timestamp").alias("last_purchase_date")
)

# 4. WRITE TO GOLD
df_customer_360.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://olist-data/gold/customer_360")

print("Success! Customer 360 Table built in Gold Layer.")

# Let's peek at the final product! Sort by highest spenders.
df_customer_360.orderBy(col("total_lifetime_value").desc()).show(5)
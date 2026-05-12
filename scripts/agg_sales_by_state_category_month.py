from pyspark.sql.functions import col, sum as _sum, month, year, round, count
from spark_utils import get_spark_session

spark = get_spark_session("Gold_Sales_Cube")
print("--- Starting Gold Layer: Sales Cube ---")

df_orders = spark.read.format("delta").load("s3a://olist-data/silver/orders")
df_items = spark.read.format("delta").load("s3a://olist-data/silver/order_items")
df_customers = spark.read.format("delta").load("s3a://olist-data/silver/customers")
df_products = spark.read.format("delta").load("s3a://olist-data/silver/products")

df_full = df_items \
    .join(df_orders.select("order_id", "customer_id", "order_purchase_timestamp"), on="order_id", how="inner") \
    .join(df_customers.select("customer_id", "customer_state"), on="customer_id", how="inner") \
    .join(df_products.select("product_id", "product_category_name"), on="product_id", how="left")

df_cube = df_full.groupBy(
    year("order_purchase_timestamp").alias("order_year"),
    month("order_purchase_timestamp").alias("order_month"),
    col("customer_state"),
    col("product_category_name")
).agg(
    round(_sum("price"), 2).alias("total_sales"),
    count("product_id").alias("total_items_sold")
)

df_cube.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://olist-data/gold/agg_sales_by_state_category_month")
    # .option("overwriteSchema", "true") \

print("Success! Sales Cube saved.")
# df_cube.orderBy(col("total_sales").desc()).show(5)
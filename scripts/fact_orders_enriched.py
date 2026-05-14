from pyspark.sql.functions import col, sum as _sum, count, datediff, round as spark_round
from delta.tables import DeltaTable
from spark_utils import get_spark_session
import sys

spark = get_spark_session("Gold_Fact_Orders")
print("--- Starting Gold Layer: Fact Orders Enriched (Incremental) ---")

GOLD_PATH = "s3a://olist-data/gold/fact_orders_enriched"

# LOAD SILVER DATA
df_orders = spark.read.format("delta").load("s3a://olist-data/silver/orders")
df_items = spark.read.format("delta").load("s3a://olist-data/silver/order_items")
df_reviews = spark.read.format("delta").load("s3a://olist-data/silver/reviews")
df_customers = spark.read.format("delta").load("s3a://olist-data/silver/customers")

# --- 1. INCREMENTAL WATERMARK (Strategy C: orders are fact-grained & append-only) ---
gold_exists = DeltaTable.isDeltaTable(spark, GOLD_PATH)
if gold_exists:
    max_ingested = spark.read.format("delta").load(GOLD_PATH) \
        .selectExpr("max(_ingested_at)").collect()[0][0]
    if max_ingested:
        df_orders = df_orders.filter(col("_ingested_at") > max_ingested)

if df_orders.rdd.isEmpty():
    print("No new/updated orders to process. Pipeline finished.")
    sys.exit(0)

print(f"Processing {df_orders.count()} affected order_id(s)...")

# AGGREGATE ITEMS (restrict to affected orders to avoid full-table scan)
affected_orders = df_orders.select("order_id")
df_items_agg = df_items.join(affected_orders, on="order_id", how="inner") \
    .groupBy("order_id").agg(
        spark_round(_sum("price"), 2).alias("total_revenue"),
        spark_round(_sum("freight_value"), 2).alias("total_freight"),
        count("product_id").alias("total_items")
    )

# ENRICH ORDERS (Join & SLA Calculations)
df_fact_orders = df_orders \
    .join(df_items_agg, on="order_id", how="inner") \
    .join(df_reviews.select("order_id", "review_score"), on="order_id", how="left") \
    .join(df_customers.select("customer_id", "customer_city", "customer_state"), on="customer_id", how="left") \
    .withColumn("days_to_deliver", datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))) \
    .withColumn("delivery_delay_days", datediff(col("order_delivered_customer_date"), col("order_estimated_delivery_date")))

# DQ L3 CHECK
null_purchases = df_fact_orders.filter(col("order_purchase_timestamp").isNull()).count()
if null_purchases > 0:
    raise Exception(f"DQ L3 FAIL: Found {null_purchases} orders missing a purchase date!")

# --- 2. MERGE OR INITIAL OVERWRITE ---
if gold_exists:
    print("Target exists. Performing MERGE on order_id...")
    gold_table = DeltaTable.forPath(spark, GOLD_PATH)
    gold_table.alias("target").merge(
        df_fact_orders.alias("source"),
        "target.order_id = source.order_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    print("Target does not exist. Performing initial OVERWRITE...")
    df_fact_orders.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(GOLD_PATH)

print("Success! Fact Orders Enriched synced.")

from pyspark.sql.functions import (
    col, sum as _sum, count, datediff, round as spark_round, current_timestamp,
)
from delta.tables import DeltaTable
from spark_utils import get_spark_session
import sys

spark = get_spark_session("Gold_Fact_Orders")
print("--- Starting Gold Layer: Fact Orders Enriched (Incremental, Affected-Orders) ---")

GOLD_PATH = "s3a://olist-data/gold/fact_orders_enriched"

# LOAD SILVER
df_orders    = spark.read.format("delta").load("s3a://olist-data/silver/orders")
df_items     = spark.read.format("delta").load("s3a://olist-data/silver/order_items")
df_reviews   = spark.read.format("delta").load("s3a://olist-data/silver/reviews")
df_customers = spark.read.format("delta").load("s3a://olist-data/silver/customers")

# --- 1. DETERMINE GOLD WATERMARK ---
gold_exists = DeltaTable.isDeltaTable(spark, GOLD_PATH)
gold_max_ts = None
if gold_exists:
    gold_cols = spark.read.format("delta").load(GOLD_PATH).columns
    if "_last_updated_at" in gold_cols:
        gold_max_ts = spark.read.format("delta").load(GOLD_PATH) \
            .selectExpr("max(_last_updated_at)").collect()[0][0]
    else:
        # Legacy Gold without _last_updated_at: missed late-arriving reviews/customer
        # changes for existing orders. Force rebuild to converge.
        print("Legacy Gold detected (no _last_updated_at). Forcing initial rebuild...")
        gold_exists = False

# --- 2. COLLECT AFFECTED order_id SET ---
if gold_max_ts is not None:
    print(f"Last Gold update: {gold_max_ts}. Scanning Silver for affected orders...")

    # Direct: orders / items / reviews changes touch their own order_id
    aff_orders  = df_orders.filter(col("_ingested_at") > gold_max_ts).select("order_id")
    aff_items   = df_items.filter(col("_ingested_at") > gold_max_ts).select("order_id")
    aff_reviews = df_reviews.filter(col("_ingested_at") > gold_max_ts).select("order_id")

    # Indirect: a customer change cascades to every order placed by that customer
    aff_customers = df_customers.filter(col("_ingested_at") > gold_max_ts).select("customer_id")
    aff_orders_via_cust = aff_customers.join(
        df_orders.select("order_id", "customer_id"), on="customer_id", how="inner",
    ).select("order_id")

    affected_orders = aff_orders \
        .unionByName(aff_items) \
        .unionByName(aff_reviews) \
        .unionByName(aff_orders_via_cust) \
        .distinct()

    if affected_orders.rdd.isEmpty():
        print("No affected orders. Pipeline finished.")
        sys.exit(0)

    print(f"Found {affected_orders.count()} affected order_id(s). Re-processing only those...")
    df_orders_scope = df_orders.join(affected_orders, on="order_id", how="inner")
else:
    print("Initial build: processing ALL orders.")
    df_orders_scope = df_orders

# --- 3. AGGREGATE ITEMS (restricted to affected orders) ---
scope_keys = df_orders_scope.select("order_id")
df_items_agg = df_items.join(scope_keys, on="order_id", how="inner") \
    .groupBy("order_id").agg(
        spark_round(_sum("price"), 2).alias("total_revenue"),
        spark_round(_sum("freight_value"), 2).alias("total_freight"),
        count("product_id").alias("total_items"),
    )

# --- 4. ENRICH & DERIVE SLA ---
df_fact_orders = df_orders_scope \
    .join(df_items_agg, on="order_id", how="inner") \
    .join(df_reviews.select("order_id", "review_score"), on="order_id", how="left") \
    .join(df_customers.select("customer_id", "customer_city", "customer_state"), on="customer_id", how="left") \
    .withColumn("days_to_deliver", datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))) \
    .withColumn("delivery_delay_days", datediff(col("order_delivered_customer_date"), col("order_estimated_delivery_date"))) \
    .withColumn("_last_updated_at", current_timestamp())

# --- 5. DQ L3 CHECK ---
null_purchases = df_fact_orders.filter(col("order_purchase_timestamp").isNull()).count()
if null_purchases > 0:
    raise Exception(f"DQ L3 FAIL: Found {null_purchases} orders missing a purchase date!")

# --- 6. MERGE OR INITIAL OVERWRITE ---
if gold_exists:
    print("Target exists. Performing MERGE on order_id...")
    gold_table = DeltaTable.forPath(spark, GOLD_PATH)
    gold_table.alias("target").merge(
        df_fact_orders.alias("source"),
        "target.order_id = source.order_id",
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    print("Target does not exist. Performing initial OVERWRITE...")
    df_fact_orders.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(GOLD_PATH)

print("Success! Fact Orders Enriched synced.")

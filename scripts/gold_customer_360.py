from pyspark.sql.functions import (
    col, sum as _sum, countDistinct, max as spark_max,
    round as spark_round, avg, mode, count, current_timestamp,
)
from delta.tables import DeltaTable
from spark_utils import get_spark_session
import sys

spark = get_spark_session("Gold_CDP")
print("--- Starting Gold Layer: Customer 360 (Incremental, Affected-Keys) ---")

GOLD_PATH = "s3a://olist-data/gold/customer_360"

# LOAD SILVER
df_customers = spark.read.format("delta").load("s3a://olist-data/silver/customers")
df_orders    = spark.read.format("delta").load("s3a://olist-data/silver/orders")
df_items     = spark.read.format("delta").load("s3a://olist-data/silver/order_items")
df_reviews   = spark.read.format("delta").load("s3a://olist-data/silver/reviews")
df_products  = spark.read.format("delta").load("s3a://olist-data/silver/products")
df_crm       = spark.read.format("delta").load("s3a://olist-data/silver/crm")
df_zendesk   = spark.read.format("delta").load("s3a://olist-data/silver/helpdesk")
df_geo       = spark.read.format("delta").load("s3a://olist-data/silver/geolocation")

# --- 1. DETERMINE GOLD WATERMARK ---
gold_exists = DeltaTable.isDeltaTable(spark, GOLD_PATH)
gold_max_ts = None
if gold_exists:
    gold_cols = spark.read.format("delta").load(GOLD_PATH).columns
    if "_last_updated_at" in gold_cols:
        gold_max_ts = spark.read.format("delta").load(GOLD_PATH) \
            .selectExpr("max(_last_updated_at)").collect()[0][0]
    else:
        # Migration: legacy Gold (overwrite-mode) without watermark column. Force rebuild.
        print("Legacy Gold detected (no _last_updated_at). Forcing initial rebuild...")
        gold_exists = False

# --- 2. COLLECT AFFECTED customer_unique_id SET ---
if gold_max_ts is not None:
    print(f"Last Gold update: {gold_max_ts}. Scanning Silver for affected customers...")

    cust_keys = df_customers.select("customer_id", "customer_unique_id")
    crm_keys  = df_crm.select("email", "customer_unique_id")

    # Customers directly changed
    aff_customers = df_customers.filter(col("_ingested_at") > gold_max_ts) \
        .select("customer_unique_id")

    # New orders -> customer_unique_id via customers
    aff_orders = df_orders.filter(col("_ingested_at") > gold_max_ts) \
        .select("customer_id") \
        .join(cust_keys, on="customer_id") \
        .select("customer_unique_id")

    # New items -> order_id -> customer_unique_id
    aff_items = df_items.filter(col("_ingested_at") > gold_max_ts) \
        .select("order_id") \
        .join(df_orders.select("order_id", "customer_id"), on="order_id") \
        .join(cust_keys, on="customer_id") \
        .select("customer_unique_id")

    # New reviews -> order_id -> customer_unique_id
    aff_reviews = df_reviews.filter(col("_ingested_at") > gold_max_ts) \
        .select("order_id") \
        .join(df_orders.select("order_id", "customer_id"), on="order_id") \
        .join(cust_keys, on="customer_id") \
        .select("customer_unique_id")

    # New CRM identities (directly carry customer_unique_id)
    aff_crm = df_crm.filter(col("_ingested_at") > gold_max_ts) \
        .select("customer_unique_id")

    # New helpdesk tickets -> email -> customer_unique_id via CRM
    aff_helpdesk = df_zendesk.filter(col("_ingested_at") > gold_max_ts) \
        .select("email") \
        .join(crm_keys, on="email") \
        .select("customer_unique_id")

    # Geolocation changes -> zip_code_prefix -> customer_unique_id via customers.
    # Closes the loop with the silver_geolocation fix: when a zip's lat/lng is
    # re-averaged, every customer in that zip must refresh, even if they had no
    # e-commerce/CRM activity in this window.
    aff_geo = df_geo.filter(col("_ingested_at") > gold_max_ts) \
        .select(col("geolocation_zip_code_prefix").alias("customer_zip_code_prefix")) \
        .distinct() \
        .join(
            df_customers.select("customer_zip_code_prefix", "customer_unique_id"),
            on="customer_zip_code_prefix",
        ) \
        .select("customer_unique_id")

    affected_keys = aff_customers \
        .unionByName(aff_orders) \
        .unionByName(aff_items) \
        .unionByName(aff_reviews) \
        .unionByName(aff_crm) \
        .unionByName(aff_helpdesk) \
        .unionByName(aff_geo) \
        .distinct()

    if affected_keys.rdd.isEmpty():
        print("No affected customers. Pipeline finished.")
        sys.exit(0)

    n_affected = affected_keys.count()
    print(f"Found {n_affected} affected customer_unique_id(s). Re-aggregating only those...")

    # Restrict customers to affected scope (limits all downstream joins)
    df_customers_scope = df_customers.join(affected_keys, on="customer_unique_id", how="inner")
else:
    print("Initial build: aggregating ALL customers.")
    df_customers_scope = df_customers

# --- 3. RE-AGGREGATE (only affected customers) ---
df_ecommerce_full = df_orders \
    .join(df_items, on="order_id", how="inner") \
    .join(df_customers_scope, on="customer_id", how="inner") \
    .join(df_products, on="product_id", how="left") \
    .join(df_reviews, on="order_id", how="left")

df_ecommerce_agg = df_ecommerce_full.groupBy(
    "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"
).agg(
    countDistinct("order_id").alias("total_orders"),
    spark_round(_sum(col("price") + col("freight_value")), 2).alias("total_lifetime_value"),
    spark_max("order_purchase_timestamp").alias("last_purchase_date"),
    spark_round(avg("review_score"), 1).alias("average_review_score"),
    mode("product_category_name").alias("favorite_category"),
)

# Support metrics: restrict zendesk to emails of affected customers (via CRM)
scope_emails = df_crm.join(
    df_customers_scope.select("customer_unique_id").distinct(),
    on="customer_unique_id", how="inner",
).select("email").distinct()

df_support_agg = df_zendesk.join(scope_emails, on="email", how="inner") \
    .groupBy("email").agg(
        count("ticket_id").alias("total_support_tickets"),
        avg("satisfaction_rating").alias("avg_support_rating"),
    )

# MULTI-CHANNEL MERGE
df_customer_360_new = df_ecommerce_agg \
    .join(df_crm, on="customer_unique_id", how="left") \
    .join(df_support_agg, on="email", how="left") \
    .join(
        df_geo,
        df_ecommerce_agg.customer_zip_code_prefix == df_geo.geolocation_zip_code_prefix,
        "left",
    ) \
    .fillna({"total_support_tickets": 0}) \
    .drop("geolocation_zip_code_prefix", "customer_zip_code_prefix") \
    .withColumn("_last_updated_at", current_timestamp())

# --- 4. MERGE OR INITIAL OVERWRITE ---
if gold_exists:
    print("Target exists. Performing MERGE on customer_unique_id...")
    gold_table = DeltaTable.forPath(spark, GOLD_PATH)
    gold_table.alias("target").merge(
        df_customer_360_new.alias("source"),
        "target.customer_unique_id = source.customer_unique_id",
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    print("Target does not exist. Performing initial OVERWRITE...")
    df_customer_360_new.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(GOLD_PATH)

# DQ L3 SANITY (post-merge)
gold_count = spark.read.format("delta").load(GOLD_PATH).count()
crm_count = df_crm.count()
if gold_count < crm_count:
    print(f"DQ L3 WARNING: Gold rows ({gold_count}) < CRM rows ({crm_count}).")

print("Success! Multi-Channel Customer 360 synced.")

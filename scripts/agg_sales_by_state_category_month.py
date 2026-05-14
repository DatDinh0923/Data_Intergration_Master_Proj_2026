from pyspark.sql.functions import (
    col, sum as _sum, month, year, round as spark_round, count, current_timestamp,
)
from delta.tables import DeltaTable
from spark_utils import get_spark_session
import sys

spark = get_spark_session("Gold_Sales_Cube")
print("--- Starting Gold Layer: Sales Cube (Incremental, Affected-Cells) ---")

GOLD_PATH = "s3a://olist-data/gold/agg_sales_by_state_category_month"

# Load Silver & rename per-source _ingested_at to avoid post-join collisions
df_orders = spark.read.format("delta").load("s3a://olist-data/silver/orders") \
    .withColumnRenamed("_ingested_at", "_orders_ts")
df_items = spark.read.format("delta").load("s3a://olist-data/silver/order_items") \
    .withColumnRenamed("_ingested_at", "_items_ts")
df_customers = spark.read.format("delta").load("s3a://olist-data/silver/customers") \
    .withColumnRenamed("_ingested_at", "_cust_ts")
df_products = spark.read.format("delta").load("s3a://olist-data/silver/products") \
    .withColumnRenamed("_ingested_at", "_prod_ts")

# Build the canonical full-join dataset (used both for affected-cell detection & re-aggregation)
df_full = df_items.select("order_id", "product_id", "price", "_items_ts") \
    .join(
        df_orders.select("order_id", "customer_id", "order_purchase_timestamp", "_orders_ts"),
        on="order_id", how="inner",
    ) \
    .join(
        df_customers.select("customer_id", "customer_state", "_cust_ts"),
        on="customer_id", how="inner",
    ) \
    .join(
        df_products.select("product_id", "product_category_name", "_prod_ts"),
        on="product_id", how="left",
    ) \
    .withColumn("order_year",  year("order_purchase_timestamp")) \
    .withColumn("order_month", month("order_purchase_timestamp"))

# --- 1. DETERMINE GOLD WATERMARK ---
gold_exists = DeltaTable.isDeltaTable(spark, GOLD_PATH)
gold_max_ts = None
if gold_exists:
    gold_cols = spark.read.format("delta").load(GOLD_PATH).columns
    if "_last_updated_at" in gold_cols:
        gold_max_ts = spark.read.format("delta").load(GOLD_PATH) \
            .selectExpr("max(_last_updated_at)").collect()[0][0]
    else:
        print("Legacy Gold detected (no _last_updated_at). Forcing initial rebuild...")
        gold_exists = False

CELL_KEYS = ["order_year", "order_month", "customer_state", "product_category_name"]

# --- 2. FIND AFFECTED CELLS ---
if gold_max_ts is not None:
    print(f"Last Gold update: {gold_max_ts}. Scanning Silver for affected cells...")

    affected_cells = df_full.filter(
        (col("_items_ts")  > gold_max_ts) |
        (col("_orders_ts") > gold_max_ts) |
        (col("_cust_ts")   > gold_max_ts) |
        (col("_prod_ts")   > gold_max_ts)
    ).select(*CELL_KEYS).distinct()

    if affected_cells.rdd.isEmpty():
        print("No affected cells. Pipeline finished.")
        sys.exit(0)

    print(f"Found {affected_cells.count()} affected cell(s). Re-aggregating only those...")

    # Restrict df_full to rows whose cell-key is in the affected set
    df_full_scope = df_full.join(affected_cells, on=CELL_KEYS, how="inner")
else:
    print("Initial build: aggregating ALL cells.")
    df_full_scope = df_full

# --- 3. RE-AGGREGATE ---
df_cube_new = df_full_scope.groupBy(*CELL_KEYS).agg(
    spark_round(_sum("price"), 2).alias("total_sales"),
    count("product_id").alias("total_items_sold"),
).withColumn("_last_updated_at", current_timestamp())

# --- 4. MERGE OR INITIAL OVERWRITE ---
if gold_exists:
    print("Target exists. Performing MERGE on (year, month, state, category)...")
    gold_table = DeltaTable.forPath(spark, GOLD_PATH)
    # Use null-safe equals (<=>) for product_category_name (may be NULL via left join)
    merge_condition = (
        "target.order_year = source.order_year AND "
        "target.order_month = source.order_month AND "
        "target.customer_state = source.customer_state AND "
        "target.product_category_name <=> source.product_category_name"
    )
    gold_table.alias("target").merge(
        df_cube_new.alias("source"),
        merge_condition,
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    print("Target does not exist. Performing initial OVERWRITE...")
    df_cube_new.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(GOLD_PATH)

print("Success! Sales Cube synced.")

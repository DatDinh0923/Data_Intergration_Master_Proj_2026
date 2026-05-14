from pyspark.sql.functions import (
    col, sum as _sum, month, year, round as spark_round, count, current_timestamp, expr,
)
from delta.tables import DeltaTable
from spark_utils import get_spark_session
import sys

spark = get_spark_session("Gold_Sales_Cube")
print("--- Starting Gold Layer: Sales Cube (Incremental, Re-Keying Safe) ---")

GOLD_PATH    = "s3a://olist-data/gold/agg_sales_by_state_category_month"
LINEAGE_PATH = "s3a://olist-data/gold/_agg_sales_cube_lineage"

# Load Silver & rename per-source _ingested_at to avoid post-join collisions
df_orders = spark.read.format("delta").load("s3a://olist-data/silver/orders") \
    .withColumnRenamed("_ingested_at", "_orders_ts")
df_items = spark.read.format("delta").load("s3a://olist-data/silver/order_items") \
    .withColumnRenamed("_ingested_at", "_items_ts")
df_customers = spark.read.format("delta").load("s3a://olist-data/silver/customers") \
    .withColumnRenamed("_ingested_at", "_cust_ts")
df_products = spark.read.format("delta").load("s3a://olist-data/silver/products") \
    .withColumnRenamed("_ingested_at", "_prod_ts")

ITEM_KEYS = ["order_id", "order_item_id"]
CELL_KEYS = ["order_year", "order_month", "customer_state", "product_category_name"]

# Canonical item-grain wide dataset (used for both affected detection & re-aggregation)
df_full = df_items.select("order_id", "order_item_id", "product_id", "price", "_items_ts") \
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

# --- 1. WATERMARK + LINEAGE PRESENCE ---
gold_exists    = DeltaTable.isDeltaTable(spark, GOLD_PATH)
lineage_exists = DeltaTable.isDeltaTable(spark, LINEAGE_PATH)
gold_max_ts = None
if gold_exists:
    gold_cols = spark.read.format("delta").load(GOLD_PATH).columns
    if "_last_updated_at" in gold_cols:
        gold_max_ts = spark.read.format("delta").load(GOLD_PATH) \
            .selectExpr("max(_last_updated_at)").collect()[0][0]
    else:
        print("Legacy Gold detected (no _last_updated_at). Forcing initial rebuild...")
        gold_exists = False
        lineage_exists = False

# Lineage is mandatory for re-keying safety. If gold exists but lineage doesn't
# (older runs before this fix), force a rebuild so we can seed the lineage table.
if gold_exists and not lineage_exists:
    print("Lineage table missing. Forcing initial rebuild to seed lineage...")
    gold_exists = False

# --- 2. AFFECTED ITEMS -> OLD + NEW CELLS ---
if gold_max_ts is not None:
    print(f"Last Gold update: {gold_max_ts}. Scanning Silver for affected items...")

    affected_items = df_full.filter(
        (col("_items_ts")  > gold_max_ts) |
        (col("_orders_ts") > gold_max_ts) |
        (col("_cust_ts")   > gold_max_ts) |
        (col("_prod_ts")   > gold_max_ts)
    ).select(*ITEM_KEYS).distinct()

    if affected_items.rdd.isEmpty():
        print("No affected items. Pipeline finished.")
        sys.exit(0)

    print(f"Found {affected_items.count()} affected item(s). Resolving old + new cells...")

    # OLD cells: where these items used to live (from the last build).
    df_lineage = spark.read.format("delta").load(LINEAGE_PATH)
    old_cells = df_lineage.join(affected_items, on=ITEM_KEYS, how="inner") \
        .select(*CELL_KEYS).distinct()

    # NEW cells: where these items live now (after Silver MERGE).
    new_cells = df_full.join(affected_items, on=ITEM_KEYS, how="inner") \
        .select(*CELL_KEYS).distinct()

    # Re-aggregating BOTH sets removes the re-keying artefact (stale contribution
    # in the old cell + missing contribution in the new cell).
    affected_cells = old_cells.unionByName(new_cells).distinct()

    # Null-safe join so cells with NULL product_category_name still match.
    join_expr = " AND ".join([f"f.{k} <=> c.{k}" for k in CELL_KEYS])
    df_full_scope = df_full.alias("f").join(
        affected_cells.alias("c"), expr(join_expr), "inner",
    ).select("f.*")
else:
    print("Initial build: aggregating ALL cells.")
    df_full_scope = df_full

# --- 3. RE-AGGREGATE ---
df_cube_new = df_full_scope.groupBy(*CELL_KEYS).agg(
    spark_round(_sum("price"), 2).alias("total_sales"),
    count("product_id").alias("total_items_sold"),
).withColumn("_last_updated_at", current_timestamp())

# --- 4. MERGE OR INITIAL OVERWRITE — CUBE ---
if gold_exists:
    print("Target exists. Performing MERGE on (year, month, state, category)...")
    gold_table = DeltaTable.forPath(spark, GOLD_PATH)
    merge_condition = (
        "target.order_year = source.order_year AND "
        "target.order_month = source.order_month AND "
        "target.customer_state = source.customer_state AND "
        "target.product_category_name <=> source.product_category_name"
    )
    gold_table.alias("target").merge(
        df_cube_new.alias("source"), merge_condition,
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    print("Target does not exist. Performing initial OVERWRITE...")
    df_cube_new.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(GOLD_PATH)

# --- 5. REFRESH LINEAGE ---
# Maps each (order_id, order_item_id) to its CURRENT cell so the next incremental
# run can detect re-keying. Without this, a customer/state or product/category
# change would update the new cell but leave the old cell's aggregate stale.
if gold_exists:
    print("Refreshing lineage for affected items...")
    df_lineage_new = df_full.join(affected_items, on=ITEM_KEYS, how="inner") \
        .select(*ITEM_KEYS, *CELL_KEYS)
    lineage_table = DeltaTable.forPath(spark, LINEAGE_PATH)
    lineage_table.alias("target").merge(
        df_lineage_new.alias("source"),
        "target.order_id = source.order_id AND target.order_item_id = source.order_item_id",
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    print("Seeding lineage from full join...")
    df_full.select(*ITEM_KEYS, *CELL_KEYS) \
        .write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(LINEAGE_PATH)

print("Success! Sales Cube synced.")

from pyspark.sql.functions import col, lower, translate
from delta.tables import DeltaTable
from spark_utils import get_spark_session

spark = get_spark_session("Silver_Customers")
BRONZE_PATH = "s3a://olist-data/bronze/olist_customers"
SILVER_PATH = "s3a://olist-data/silver/customers"

df_customers_bronze = spark.read.format("delta").load(BRONZE_PATH)

# --- 1. INCREMENTAL FILTERING (The Watermark) ---
if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    max_ingested = spark.read.format("delta").load(SILVER_PATH).selectExpr("max(_ingested_at)").collect()[0][0]
    if max_ingested:
        df_customers_bronze = df_customers_bronze.filter(col("_ingested_at") > max_ingested)

# If no new data, stop early to save compute
if df_customers_bronze.count() == 0:
    print("No new data to process. Pipeline finished.")
    import sys
    sys.exit(0)

# --- 2. TRANSFORM & CLEAN ---
REQUIRED_COLUMNS = ["customer_id", "customer_unique_id", "customer_city", "customer_state"]
missing_cols = [c for c in REQUIRED_COLUMNS if c not in df_customers_bronze.columns]
if missing_cols:
    raise Exception(f"FATAL: Source missing mandatory columns: {missing_cols}. Halting pipeline!")

df_cleaned = df_customers_bronze \
    .withColumn("customer_city", lower(col("customer_city"))) \
    .withColumn("customer_city", translate(col("customer_city"), "áàâãäéèêëíìîïóòôõöúùûüç", "aaaaaeeeeiiiiooooouuuuc"))

# DQ CHECKS
null_id_count = df_cleaned.filter(col("customer_id").isNull()).count()
if null_id_count > 0:
    raise Exception(f"DQ FAIL: Found {null_id_count} rows with a null customer_id.")

final_columns = [
    "customer_id", "customer_unique_id", "customer_zip_code_prefix", 
    "customer_city", "customer_state", "_ingested_at", "_source_file"
]
df_cleaned = df_cleaned.select(*final_columns)

# --- 3. MERGE OR OVERWRITE ---
if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    print("Target exists. Performing MERGE (Upsert)...")
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)
    silver_table.alias("target").merge(
        df_cleaned.alias("source"),
        "target.customer_id = source.customer_id"  # Primary Key Match
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
else:
    print("Target does not exist. Performing initial OVERWRITE...")
    df_cleaned.write.format("delta").mode("overwrite").save(SILVER_PATH)

print("Successfully synced Customers to Silver.")
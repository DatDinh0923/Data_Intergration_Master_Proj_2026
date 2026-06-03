from pyspark.sql.functions import col
from delta.tables import DeltaTable
from spark_utils import get_spark_session
import sys

spark = get_spark_session("Silver_Sellers")
BRONZE_PATH = "s3a://olist-data/bronze/olist_sellers"
SILVER_PATH = "s3a://olist-data/silver/sellers"

df_bronze = spark.read.format("delta").load(BRONZE_PATH)

# INCREMENTAL WATERMARK
if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    max_ingested = spark.read.format("delta").load(SILVER_PATH).selectExpr("max(_ingested_at)").collect()[0][0]
    if max_ingested:
        df_bronze = df_bronze.filter(col("_ingested_at") > max_ingested)

if df_bronze.count() == 0:
    print("No new data to process. Pipeline finished.")
    sys.exit(0)

# TRANSFORM & CLEAN
REQUIRED_COLUMNS = ["seller_id", "seller_city", "seller_state"]
missing_cols = [c for c in REQUIRED_COLUMNS if c not in df_bronze.columns]
if missing_cols:
    raise Exception(f"FATAL: Source missing mandatory columns: {missing_cols}. Halting pipeline!")

null_sellers = df_bronze.filter(col("seller_id").isNull()).count()
if null_sellers > 0:
    raise Exception(f"DQ L2 FAIL: Found {null_sellers} missing Seller IDs!")

final_columns = [
    "seller_id", "seller_zip_code_prefix", "seller_city", 
    "seller_state", "_ingested_at", "_source_file"
]
df_clean = df_bronze.select(*final_columns)

if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    print("Target exists. Performing MERGE...")
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)
    silver_table.alias("target").merge(
        df_clean.alias("source"), "target.seller_id = source.seller_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    print("Target does not exist. Performing OVERWRITE...")
    df_clean.write.format("delta").mode("overwrite").save(SILVER_PATH)

print("Successfully synced Sellers to Silver.")
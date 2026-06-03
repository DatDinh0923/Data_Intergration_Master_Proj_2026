from pyspark.sql.functions import col, lower
from delta.tables import DeltaTable
from spark_utils import get_spark_session
import sys

spark = get_spark_session("Silver_Products")
BRONZE_PATH = "s3a://olist-data/bronze/olist_products"
SILVER_PATH = "s3a://olist-data/silver/products"

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
PRODUCT_COLUMNS = [
    "product_id",
    "product_category_name",
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
    "_ingested_at",
    "_source_file",
]
# Keep only columns that actually exist in Bronze (defensive against schema drift)
present_cols = [c for c in PRODUCT_COLUMNS if c in df_bronze.columns]
df_clean = df_bronze.select(*present_cols) \
    .withColumn("product_category_name", lower(col("product_category_name"))) \
    .fillna("unknown", subset=["product_category_name"])

nulls = df_clean.filter(col("product_id").isNull()).count()
if nulls > 0:
    raise Exception(f"DQ FAIL: Found {nulls} missing Product IDs!")

if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    print("Target exists. Performing MERGE...")
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)
    silver_table.alias("target").merge(
        df_clean.alias("source"), "target.product_id = source.product_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    print("Target does not exist. Performing OVERWRITE...")
    df_clean.write.format("delta").mode("overwrite").save(SILVER_PATH)

print("Successfully synced Products to Silver.")
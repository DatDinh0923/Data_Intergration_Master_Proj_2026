from pyspark.sql.functions import col
from spark_utils import get_spark_session

spark = get_spark_session("Silver_Sellers")
df_bronze = spark.read.format("delta").load("s3a://olist-data/bronze/olist_sellers")

REQUIRED_COLUMNS = ["seller_id", "seller_city", "seller_state"]
missing_cols = [c for c in REQUIRED_COLUMNS if c not in df_bronze.columns]
if missing_cols:
    raise Exception(f"FATAL: Source missing mandatory columns: {missing_cols}. Halting pipeline!")

# DQ CHECK: Ensure seller_id is not null
null_sellers = df_bronze.filter(col("seller_id").isNull()).count()
if null_sellers > 0:
    raise Exception(f"DQ L2 FAIL: Found {null_sellers} missing Seller IDs!")

final_columns = [
    "seller_id", "seller_zip_code_prefix", "seller_city", 
    "seller_state", "_ingested_at", "_source_file"
]

df_bronze.select(*final_columns).write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save("s3a://olist-data/silver/sellers")

print("Successfully wrote to Silver")
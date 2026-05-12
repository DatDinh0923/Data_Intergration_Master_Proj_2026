from pyspark.sql.functions import col
from spark_utils import get_spark_session

spark = get_spark_session("Silver_CRM")
df_bronze = spark.read.format("delta").load("s3a://olist-data/bronze/crm_identities")

REQUIRED_COLUMNS = ["customer_unique_id", "email"]
missing_cols = [c for c in REQUIRED_COLUMNS if c not in df_bronze.columns]
if missing_cols:
    raise Exception(f"FATAL: Source missing mandatory columns: {missing_cols}. Halting pipeline!")

# DQ CHECK: Ensure every CRM record has an email (Semantic check)
null_emails = df_bronze.filter(col("email").isNull()).count()
if null_emails > 0:
    raise Exception(f"DQ L2 FAIL: Found {null_emails} users missing an email address in the CRM system!")

final_columns = [
    "customer_unique_id", "email", "_ingested_at", "_source_file"
]

df_bronze.select(*final_columns).write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("s3a://olist-data/silver/crm")

print("Successfully wrote to Silver")
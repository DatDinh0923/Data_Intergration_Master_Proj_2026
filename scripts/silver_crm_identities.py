from pyspark.sql.functions import col
from spark_utils import get_spark_session

spark = get_spark_session("Silver_CRM")
print("--- Starting Silver Pipeline: CRM Identities ---")

df_bronze = spark.read.format("delta").load("s3a://olist-data/bronze/crm_identities")

# DQ CHECK: Ensure every CRM record has an email (Semantic check)
null_emails = df_bronze.filter(col("email").isNull()).count()
if null_emails > 0:
    raise Exception(f"DQ L2 FAIL: Found {null_emails} users missing an email address in the CRM system!")

df_bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://olist-data/silver/crm")
print("Success: CRM Identities written to Silver.")
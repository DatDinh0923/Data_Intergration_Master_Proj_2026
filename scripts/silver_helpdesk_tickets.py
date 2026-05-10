from pyspark.sql.functions import col
from spark_utils import get_spark_session

spark = get_spark_session("Silver_Zendesk")
print("--- Starting Silver Pipeline: Zendesk Support ---")

df_bronze = spark.read.format("delta").load("s3a://olist-data/bronze/zendesk_tickets")

# 1. TRANSFORM & CLEAN: Drop any internal metadata columns
df_clean = df_bronze.select("ticket_id", "email", "issue_type", col("satisfaction_rating").cast("int"))

# 2. DQ CHECK: Ensure satisfaction rating is strictly between 1 and 5
invalid_ratings = df_clean.filter((col("satisfaction_rating") < 1) | (col("satisfaction_rating") > 5)).count()
if invalid_ratings > 0:
    raise Exception(f"DQ L2 FAIL: Found {invalid_ratings} support tickets with ratings outside the 1-5 scale!")

df_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://olist-data/silver/zendesk")
print("Success: Helpdesk Tickets written to Silver.")
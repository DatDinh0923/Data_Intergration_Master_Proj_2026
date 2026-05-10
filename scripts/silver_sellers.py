from pyspark.sql.functions import col
from spark_utils import get_spark_session

spark = get_spark_session("Silver_Sellers")
print("--- Starting Silver Pipeline: Sellers ---")

df_bronze = spark.read.format("delta").load("s3a://olist-data/bronze/olist_sellers")

# DQ CHECK: Ensure seller_id is not null
null_sellers = df_bronze.filter(col("seller_id").isNull()).count()
if null_sellers > 0:
    raise Exception(f"DQ L2 FAIL: Found {null_sellers} missing Seller IDs!")

df_bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://olist-data/silver/sellers")
print("Success: Sellers written to Silver.")
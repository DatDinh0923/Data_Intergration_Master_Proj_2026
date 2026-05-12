from pyspark.sql.functions import col
from spark_utils import get_spark_session

spark = get_spark_session("Silver_Reviews")

print("--- Starting Silver Pipeline: Reviews ---")
# df_bronze = spark.read.format("csv") \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .load("s3a://olist-data/bronze/olist_order_reviews_dataset.csv")
df_bronze = spark.read.format("delta").load("s3a://olist-data/bronze/olist_reviews")

# Clean: Select only what we need, enforce types, filter bad scores
df_clean = df_bronze.select("review_id", "order_id", col("review_score").cast("int")) \
    .filter((col("review_score") >= 1) & (col("review_score") <= 5))

# DQ Check: No missing IDs
print("Running DQ Checks for olist_order_reviews_dataset.csv ...")
print("DQ Check: If there is any NULL value in neither review_id nor order_id ...")

nulls = df_clean.filter(col("review_id").isNull() | col("order_id").isNull()).count()
if nulls > 0:
    raise Exception(f"DQ FAIL: Found {nulls} missing IDs in Reviews!")
print("DQ Checks Passed! Proceeding to write to Silver.")

# df_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://olist-data/silver/reviews")
df_clean.write.format("delta").mode("overwrite").save("s3a://olist-data/silver/reviews")
print("Successfully wrote to Silver")
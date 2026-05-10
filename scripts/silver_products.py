from pyspark.sql.functions import col, lower
from spark_utils import get_spark_session

spark = get_spark_session("Silver_Products")

# df_bronze = spark.read.format("csv") \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .load("s3a://olist-data/bronze/olist_products_dataset.csv")

df_bronze = spark.read.format("delta").load("s3a://olist-data/bronze/olist_products")


# 1. Clean: Lowercase strings, and fill NULLs with 'unknown'
df_clean = df_bronze.select("product_id", "product_category_name") \
    .withColumn("product_category_name", lower(col("product_category_name"))) \
    .fillna("unknown", subset=["product_category_name"])

# 2. DQ Check: Ensure Primary Key exists
print("Running DQ Checks olist_products_dataset.csv ...")
print("DQ Check: If there is any NULL value in product_id ...")
nulls = df_clean.filter(col("product_id").isNull()).count()
if nulls > 0:
    raise Exception(f"DQ FAIL: Found {nulls} missing Product IDs!")

# 3. Write to Silver
df_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://olist-data/silver/products")
print("Success: Products written to Silver.")
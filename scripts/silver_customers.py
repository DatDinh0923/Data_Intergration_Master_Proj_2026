from pyspark.sql.functions import col, lower, translate, lower
from pyspark.sql.utils import AnalysisException
from spark_utils import get_spark_session

spark = get_spark_session("Silver_Customers")

# 1. LOAD BRONZE DATA
df_customers_bronze = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("s3a://olist-data/bronze/olist_customers_dataset.csv")

df_customers_bronze.printSchema()
df_customers_bronze.show(5)

# 2. TRANSFORM & CLEAN (Standardization)
# Lowercase the city and remove common Portuguese accents
df_cleaned = df_customers_bronze \
    .withColumn("customer_city", lower(col("customer_city"))) \
    .withColumn("customer_city", translate(col("customer_city"), "áàâãäéèêëíìîïóòôõöúùûüç", "aaaaaeeeeiiiiooooouuuuc"))

# 3. AUTOMATED DATA QUALITY (DQ) CHECKS
print("Running Automated DQ Checks olist_customers_dataset.csv ...")

# Rule A: Customer ID cannot be null
print("DQ Check: If there is any NULL value in customer_id ...")
null_id_count = df_cleaned.filter(col("customer_id").isNull()).count()
if null_id_count > 0:
    raise Exception(f"DQ FAIL: Found {null_id_count} rows with a null customer_id. Pipeline halted.")

# Rule B: Customer ID must be unique
print("DQ Check: If there are any duplicated value in customer_id ...")
total_rows = df_cleaned.count()
unique_ids = df_cleaned.select("customer_id").distinct().count()
if total_rows != unique_ids:
    raise Exception(f"DQ FAIL: Found {total_rows - unique_ids} duplicate customer_ids. Pipeline halted.")

print("DQ Checks Passed! Proceeding to write to Silver.")

# 4. WRITE TO SILVER (Enforces Schema & Enables Time Travel)
df_cleaned.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://olist-data/silver/customers")

print("Successfully wrote Customers to Silver Delta Table.")
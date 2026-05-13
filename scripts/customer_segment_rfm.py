from pyspark.sql.functions import col, datediff, to_date, lit
from spark_utils import get_spark_session

spark = get_spark_session("Gold_Customer_RFM")
print("--- Starting Gold Layer: RFM (raw values only) ---")

df_c360 = spark.read.format("delta").load("s3a://olist-data/gold/customer_360")

# R / F / M raw values:
#   R = recency_days   (computed below from last_purchase_date)
#   F = total_orders          (already in customer_360)
#   M = total_lifetime_value  (already in customer_360)
df_rfm = df_c360.withColumn(
    "recency_days",
    datediff(to_date(lit("2018-10-01")), col("last_purchase_date"))
)

df_rfm.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://olist-data/gold/segment_rfm")

print("Success! RFM raw table saved.")

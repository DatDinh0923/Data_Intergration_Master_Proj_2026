from pyspark.sql.functions import col, expr, datediff, to_date, lit
from spark_utils import get_spark_session

spark = get_spark_session("Gold_Customer_Segment_RFM")
print("--- Starting Gold Layer: RFM Segmentation ---")

df_c360 = spark.read.format("delta").load("s3a://olist-data/gold/customer_360")

# 2. Tính số ngày từ lần mua cuối cùng đến mốc thời gian (Recency)
# Dùng "2018-10-01" làm mốc tham chiếu vì dataset Olist dừng ở năm 2018.
df_rfm_base = df_c360.withColumn("recency_days", datediff(to_date(lit("2018-10-01")), col("last_purchase_date")))

# 3. Tính điểm R, F, M (Chia đều khách hàng thành 4 nhóm bằng hàm NTILE)
df_rfm_scores = df_rfm_base \
    .withColumn("R_Score", expr("ntile(4) over (order by recency_days desc)")) \
    .withColumn("F_Score", expr("ntile(4) over (order by total_orders asc)")) \
    .withColumn("M_Score", expr("ntile(4) over (order by total_lifetime_value asc)"))

df_segment = df_rfm_scores.withColumn("RFM_Segment", 
    expr("""
        CASE 
            WHEN R_Score >= 3 AND F_Score >= 3 AND M_Score >= 3 THEN '1. Champions'
            WHEN R_Score >= 2 AND F_Score >= 2 THEN '2. Loyal Customers'
            WHEN R_Score <= 2 AND F_Score >= 3 THEN '3. At Risk'
            WHEN R_Score <= 2 AND F_Score <= 2 THEN '4. Hibernating / Lost'
            ELSE '5. Potential / Others'
        END
    """)
)

# 5. Ghi ra tầng Gold
df_segment.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://olist-data/gold/segment_rfm")
    # .option("overwriteSchema", "true") \

print("Success! RFM Segmentation saved.")
# df_segment.groupBy("RFM_Segment").count().orderBy("RFM_Segment").show()
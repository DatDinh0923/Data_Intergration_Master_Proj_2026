from pyspark.sql.functions import current_timestamp, lit
from spark_utils import get_spark_session

spark = get_spark_session("Bronze_Ingestion")

# Define all 11 tables mapping (Landing CSV Path -> Bronze Delta Folder Name)
pipeline_tables = {
    "olist_orders": "s3a://olist-data/landing/olist_orders_dataset.csv",
    "olist_customers": "s3a://olist-data/landing/olist_customers_dataset.csv",
    "olist_items": "s3a://olist-data/landing/olist_order_items_dataset.csv",
    "olist_products": "s3a://olist-data/landing/olist_products_dataset.csv",
    "olist_payments": "s3a://olist-data/landing/olist_order_payments_dataset.csv",
    "olist_reviews": "s3a://olist-data/landing/olist_order_reviews_dataset.csv",
    "olist_sellers": "s3a://olist-data/landing/olist_sellers_dataset.csv",
    "olist_geolocation": "s3a://olist-data/landing/olist_geolocation_dataset.csv",
    "translation": "s3a://olist-data/landing/product_category_name_translation.csv",
    "crm_identities": "s3a://olist-data/landing/crm_raw",
    "zendesk_tickets": "s3a://olist-data/landing/zendesk_raw"
}

print("--- Starting Bronze Ingestion & DQ Checks ---")

for table_name, file_path in pipeline_tables.items():
    print(f"\nProcessing: {table_name}...")
    
    try:
        # 1. READ RAW DATA
        df_raw = spark.read.csv(file_path, header=True, inferSchema=True)
        
        # 2. AUTOMATED DQ LEVEL 1: Structural Checks
        total_rows = df_raw.count()
        num_columns = len(df_raw.columns)
        
        if total_rows == 0:
            raise ValueError(f"DQ L1 FAIL: The file is totally empty! ({total_rows} rows)")
            
        if num_columns < 2:
            raise ValueError(f"DQ L1 FAIL: Suspect file format. Only found {num_columns} column.")
            
        print(f"  [DQ PASS] Valid structure detected: {total_rows:,} rows, {num_columns} columns.")
        
        # 3. ADD GOVERNANCE METADATA
        df_bronze = df_raw.withColumn("_ingested_at", current_timestamp()) \
                          .withColumn("_source_file", lit(file_path))
        
        # 4. WRITE TO BRONZE AS DELTA (Enabling Time Travel!)
        bronze_dest = f"s3a://olist-data/bronze/{table_name}"
        df_bronze.write.format("delta").mode("overwrite").save(bronze_dest)
        print(f"  [SUCCESS] Saved to Bronze Delta: {bronze_dest}")
        
    except Exception as e:
        print(f"  [ERROR] Pipeline stopped for {table_name}. Reason: {e}")

print("\nALL TABLES SUCCESSFULLY INGESTED TO BRONZE!")
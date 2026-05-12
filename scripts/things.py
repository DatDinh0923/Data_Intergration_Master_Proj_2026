import os
import shutil
import sys
from pyspark.sql.functions import current_timestamp, input_file_name
from spark_utils import get_spark_session

spark = get_spark_session("Universal_Bronze_Ingest")

# 1. CONFIGURATION
# Root paths for your zones
LANDING_ROOT = "/opt/airflow/data/landing"
ARCHIVE_ROOT = "/opt/airflow/data/archive"
BRONZE_BASE_PATH = "s3a://olist-data/bronze"

# Mapping: Table Name -> Subfolder Name in Landing/Archive
pipeline_tables = {
    "olist_orders": "orders",
    "olist_customers": "customers",
    "olist_items": "items",
    "olist_products": "products",
    "olist_payments": "payments",
    "olist_reviews": "reviews",
    "olist_sellers": "sellers",
    "olist_geolocation": "geolocation",
    "translation": "translation",
    "crm_identities": "crm",
    "helpdesk_tickets": "helpdesk"
}

print("--- Starting Universal Incremental Bronze Ingestion ---")

for table_name, folder_name in pipeline_tables.items():
    landing_path = f"{LANDING_ROOT}/{folder_name}"
    archive_path = f"{ARCHIVE_ROOT}/{folder_name}"
    bronze_dest = f"{BRONZE_BASE_PATH}/{table_name}"
    
    # Ensure directories exist
    os.makedirs(landing_path, exist_ok=True)
    os.makedirs(archive_path, exist_ok=True)
    
    # 2. CHECK FOR FILES
    files_to_process = [f for f in os.listdir(landing_path) if f.endswith('.csv')]
    
    if not files_to_process:
        print(f"Skipping {table_name}: No new files in {landing_path}")
        continue

    print(f"Processing {table_name}: Found {len(files_to_process)} new file(s)...")

    try:
        # 3. READ & ADD METADATA
        # Using *.csv reads all files in that subfolder at once
        df_new = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(f"{landing_path}/*.csv")
            
        df_bronze = df_new \
            .withColumn("_ingested_at", current_timestamp()) \
            .withColumn("_source_file", input_file_name())

        # 4. APPEND TO BRONZE
        df_bronze.write.format("delta") \
            .mode("append") \
            .save(bronze_dest)
        
        # 5. MOVE TO ARCHIVE
        for file_name in files_to_process:
            shutil.move(f"{landing_path}/{file_name}", f"{archive_path}/{file_name}")
            
        print(f"SUCCESS: {table_name} appended to Bronze and files archived.")

    except Exception as e:
        print(f"ERROR: Failed to process {table_name}. Reason: {e}")

print("--- All tables processed ---")
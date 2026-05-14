# import sys
# import boto3
# from pyspark.sql.functions import current_timestamp, input_file_name
# from spark_utils import get_spark_session
# spark = get_spark_session("Cloud_Native_Bronze_Ingest")

# # 1. CLOUD CONFIGURATION
# BUCKET = "olist-data"
# LANDING_PREFIX = "landing"
# ARCHIVE_PREFIX = "archive"
# BRONZE_BASE_PATH = f"s3a://{BUCKET}/bronze"

# # Connect to MinIO via Boto3
# s3 = boto3.client('s3',
#     endpoint_url='http://minio:9000', # Use 'localhost:9000' if running outside Docker
#     aws_access_key_id='admin',
#     aws_secret_access_key='password',
#     region_name='us-east-1'
# )

# # Mapping: Table Name -> Subfolder Name
# pipeline_tables = {
#     "olist_orders": "orders",
#     "olist_customers": "customers",
#     "olist_items": "items",
#     "olist_products": "products",
#     "olist_payments": "payments",
#     "olist_reviews": "reviews",
#     "olist_sellers": "sellers",
#     "olist_geolocation": "geolocation",
#     "translation": "translation",
#     "crm_identities": "crm",
#     "helpdesk_tickets": "helpdesk"
# }

# print("--- Starting Cloud-Native Bronze Ingestion ---")
# total_files_found = 0
# for table_name, folder_name in pipeline_tables.items():
#     landing_folder_key = f"{LANDING_PREFIX}/{folder_name}/"
#     bronze_dest = f"{BRONZE_BASE_PATH}/{table_name}"
    
#     # 2. CHECK FOR FILES IN MINIO — paginate to avoid the 1000-key cap
#     paginator = s3.get_paginator('list_objects_v2')
#     files_to_process = []
#     for page in paginator.paginate(Bucket=BUCKET, Prefix=landing_folder_key):
#         files_to_process.extend(
#             obj['Key'] for obj in page.get('Contents', [])
#             if obj['Key'].endswith('.csv')
#         )

#     if not files_to_process:
#         print(f"Skipping {table_name}: No new files in s3://{BUCKET}/{landing_folder_key}")
#         continue

#     print(f"Processing {table_name}: Found {len(files_to_process)} new file(s)...")

#     try:
#         # 3. SPARK READS ONLY THE FILES WE LISTED (avoids race with new uploads)
#         s3a_paths = [f"s3a://{BUCKET}/{k}" for k in files_to_process]

#         df_new = spark.read.format("csv") \
#             .option("header", "true") \
#             .option("inferSchema", "true") \
#             .load(s3a_paths)
            
#         # Add Governance Metadata
#         df_bronze = df_new \
#             .withColumn("_ingested_at", current_timestamp()) \
#             .withColumn("_source_file", input_file_name())

#         # 4. APPEND TO BRONZE DELTA
#         df_bronze.write.format("delta") \
#             .mode("append") \
#             .save(bronze_dest)
        
#         # 5. MOVE TO ARCHIVE IN S3 (Replacing shutil.move)
#         for old_key in files_to_process:
#             filename = old_key.split('/')[-1]
#             new_key = f"{ARCHIVE_PREFIX}/{folder_name}/{filename}"
            
#             # Copy to archive prefix, then delete from landing prefix
#             s3.copy_object(Bucket=BUCKET, CopySource={'Bucket': BUCKET, 'Key': old_key}, Key=new_key)
#             s3.delete_object(Bucket=BUCKET, Key=old_key)
            
#         print(f"SUCCESS: {table_name} appended to Bronze and archived in MinIO.")

#     except Exception as e:
#         print(f"ERROR: Failed to process {table_name}. Reason: {e}")

# print("--- All tables processed ---")

# total_files_found += len(files_to_process)
# if total_files_found == 0:
#     print("No new data found in any landing zone. Skipping downstream tasks.")
#     sys.exit(99)
# else:
#     print(f"Successfully processed a total of {total_files_found} new file(s).")

import sys
import boto3
from pyspark.sql.functions import current_timestamp, input_file_name
from spark_utils import get_spark_session

spark = get_spark_session("Cloud_Native_Bronze_Ingest")

# 1. CLOUD CONFIGURATION
BUCKET = "olist-data"
LANDING_PREFIX = "landing"
ARCHIVE_PREFIX = "archive"
BRONZE_BASE_PATH = f"s3a://{BUCKET}/bronze"

# Connect to MinIO via Boto3
s3 = boto3.client('s3',
    endpoint_url='http://minio:9000', 
    aws_access_key_id='admin',
    aws_secret_access_key='password',
    region_name='us-east-1'
)

# Mapping: Table Name -> Subfolder Name
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

print("--- Starting Cloud-Native Bronze Ingestion ---")
total_files_found = 0

for table_name, folder_name in pipeline_tables.items():
    landing_folder_key = f"{LANDING_PREFIX}/{folder_name}/"
    bronze_dest = f"{BRONZE_BASE_PATH}/{table_name}"
    
    # 2. CHECK FOR FILES IN MINIO — paginate to avoid the 1000-key cap
    paginator = s3.get_paginator('list_objects_v2')
    files_to_process = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=landing_folder_key):
        files_to_process.extend(
            obj['Key'] for obj in page.get('Contents', [])
            if obj['Key'].endswith('.csv')
        )

    if not files_to_process:
        print(f"Skipping {table_name}: No new files in s3://{BUCKET}/{landing_folder_key}")
        continue

    # FIX 1: The counter MUST be indented inside the loop here!
    total_files_found += len(files_to_process)

    print(f"Processing {table_name}: Found {len(files_to_process)} new file(s)...")

    try:
        # 3. SPARK READS ONLY THE FILES WE LISTED (avoids race with new uploads)
        s3a_paths = [f"s3a://{BUCKET}/{k}" for k in files_to_process]

        # FIX 2: Added multiLine and escape options so it survives the messy original files
        df_new = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("multiLine", "true") \
            .option("escape", '"') \
            .load(s3a_paths)
            
        # Add Governance Metadata
        df_bronze = df_new \
            .withColumn("_ingested_at", current_timestamp()) \
            .withColumn("_source_file", input_file_name())

        # 4. APPEND TO BRONZE DELTA
        df_bronze.write.format("delta") \
            .mode("append") \
            .save(bronze_dest)
        
        # 5. MOVE TO ARCHIVE IN S3
        for old_key in files_to_process:
            filename = old_key.split('/')[-1]
            new_key = f"{ARCHIVE_PREFIX}/{folder_name}/{filename}"
            
            # Copy to archive prefix, then delete from landing prefix
            s3.copy_object(Bucket=BUCKET, CopySource={'Bucket': BUCKET, 'Key': old_key}, Key=new_key)
            s3.delete_object(Bucket=BUCKET, Key=old_key)
            
        print(f"SUCCESS: {table_name} appended to Bronze and archived in MinIO.")

    except Exception as e:
        print(f"ERROR: Failed to process {table_name}. Reason: {e}")

print("--- All tables processed ---")

# Evaluate the total files found after the loop is completely finished
if total_files_found == 0:
    print("No new data found in any landing zone. Skipping downstream tasks.")
    sys.exit(99)
else:
    print(f"Successfully processed a total of {total_files_found} new file(s).")
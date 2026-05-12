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
    endpoint_url='http://minio:9000', # Use 'localhost:9000' if running outside Docker
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

for table_name, folder_name in pipeline_tables.items():
    landing_folder_key = f"{LANDING_PREFIX}/{folder_name}/"
    bronze_dest = f"{BRONZE_BASE_PATH}/{table_name}"
    
    # 2. CHECK FOR FILES IN MINIO (Replacing os.listdir)
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=landing_folder_key)
    
    # Filter for actual .csv files (ignoring the folder marker itself)
    files_to_process = [
        obj['Key'] for obj in response.get('Contents', []) 
        if obj['Key'].endswith('.csv')
    ]
    
    if not files_to_process:
        print(f"Skipping {table_name}: No new files in s3://{BUCKET}/{landing_folder_key}")
        continue

    print(f"Processing {table_name}: Found {len(files_to_process)} new file(s)...")

    try:
        # 3. SPARK READS DIRECTLY FROM S3
        s3a_read_path = f"s3a://{BUCKET}/{landing_folder_key}*.csv"
        
        df_new = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(s3a_read_path)
            
        # Add Governance Metadata
        df_bronze = df_new \
            .withColumn("_ingested_at", current_timestamp()) \
            .withColumn("_source_file", input_file_name())

        # 4. APPEND TO BRONZE DELTA
        df_bronze.write.format("delta") \
            .mode("append") \
            .save(bronze_dest)
        
        # 5. MOVE TO ARCHIVE IN S3 (Replacing shutil.move)
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
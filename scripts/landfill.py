# import os
# import shutil

# # Path to your landing root
# LANDING_ROOT = "/opt/airflow/data/landing"

# # Mapping based on your specific filenames and folder structure
# move_map = {
#     "olist_orders": "orders",
#     "olist_customers": "customers",
#     "olist_order_items": "items",
#     "olist_products": "products",
#     "olist_order_payments": "payments",
#     "olist_order_reviews": "reviews",
#     "olist_sellers": "sellers",
#     "olist_geolocation": "geolocation",
#     "product_category_name_translation": "translation"
# }

# print("Starting one-time landing zone cleanup...")

# # List only files in the root landing directory
# for filename in os.listdir(LANDING_ROOT):
#     file_path = os.path.join(LANDING_ROOT, filename)
    
#     # Only process files, skip existing directories like 'crm' or 'helpdesk'
#     if os.path.isfile(file_path) and filename.endswith(".csv"):
#         moved = False
        
#         # Check which folder the file belongs to
#         for key, folder_name in move_map.items():
#             if key in filename:
#                 dest_dir = os.path.join(LANDING_ROOT, folder_name)
                
#                 # Ensure the subfolder exists
#                 os.makedirs(dest_dir, exist_ok=True)
                
#                 # Move the file
#                 shutil.move(file_path, os.path.join(dest_dir, filename))
#                 print(f"Moved {filename} -> {folder_name}/")
#                 moved = True
#                 break
        
#         if not moved:
#             print(f"No folder mapping found for: {filename}")

# print("Cleanup complete. You can now re-run your main Bronze script.")

import boto3

# 1. CONNECT TO MINIO
# Replace with your actual credentials and MinIO endpoint
s3 = boto3.client('s3',
    endpoint_url='http://localhost:9000', # Or your MinIO container IP
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    region_name='us-east-1'
)

BUCKET = "olist-data"
LANDING_ROOT = "landing/"

# Mapping: Substring in filename -> Target Prefix (Folder)
move_map = {
    "olist_orders": "orders/",
    "olist_customers": "customers/",
    "olist_order_items": "items/",
    "olist_products": "products/",
    "olist_order_payments": "payments/",
    "olist_order_reviews": "reviews/",
    "olist_sellers": "sellers/",
    "olist_geolocation": "geolocation/",
    "product_category_name_translation": "translation/"
}

print("Starting MinIO bucket cleanup...")

# 2. LIST ALL OBJECTS IN THE LANDING PREFIX
response = s3.list_objects_v2(Bucket=BUCKET, Prefix=LANDING_ROOT)

if 'Contents' not in response:
    print("No files found in the landing prefix.")
else:
    for obj in response['Contents']:
        old_key = obj['Key']
        filename = old_key.split('/')[-1]
        
        # Skip if the object is already in a subfolder or is a folder itself
        if not filename or '/' in old_key.replace(LANDING_ROOT, ''):
            continue
            
        # 3. MATCH FILENAME TO TARGET FOLDER
        for key, folder_name in move_map.items():
            if key in filename:
                new_key = f"{LANDING_ROOT}{folder_name}{filename}"
                
                # Copy the object to the new path
                s3.copy_object(
                    Bucket=BUCKET,
                    CopySource={'Bucket': BUCKET, 'Key': old_key},
                    Key=new_key
                )
                
                # Delete the old object from the root
                s3.delete_object(Bucket=BUCKET, Key=old_key)
                print(f"Moved in MinIO: {filename} -> {folder_name}")
                break

print("MinIO organization complete.")
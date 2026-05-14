# !pip install seaborn
# !pip install SQLAlchemy
# !pip install psycopg2-binary
# !pip install delta-spark==3.1.0

# from pyspark.sql import SparkSession

# jar_paths = [
#     "/home/jovyan/work/jars/delta-spark_2.12-3.1.0.jar",
#     "/home/jovyan/work/jars/delta-storage-3.1.0.jar",
#     "/home/jovyan/work/jars/hadoop-aws-3.3.4.jar",
#     "/home/jovyan/work/jars/aws-java-sdk-bundle-1.12.262.jar"
# ]
# jars_string = ",".join(jar_paths)

# spark = SparkSession.builder \
#     .appName("LakehouseSetup_Offline") \
#     .config("spark.jars", jars_string) \
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
#     .config("spark.hadoop.fs.s3a.access.key", "admin") \
#     .config("spark.hadoop.fs.s3a.secret.key", "password") \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .getOrCreate()

# print(f"Spark Version: {spark.version}")
# print("Connected successfully in Offline Mode! Ready to build the Lakehouse.")

# import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns
# from sqlalchemy import create_engine

# # Set plot style for academic paper
# plt.style.use('seaborn-v0_8-whitegrid')
# sns.set_context("paper", font_scale=1.2)

# # 1. Connect to Airflow Metadata DB (Update with your credentials)
# # Default docker-compose airflow connection string usually looks like this:
# # New code
# db_url = "postgresql+psycopg2://airflow:airflow@postgres-airflow:5432/airflow"
# engine = create_engine(db_url)

# # Query Task Instance Latency (9.3.1)
# query_tasks = """
# SELECT task_id, duration 
# FROM task_instance 
# WHERE dag_id = 'medallion_end_to_end_pipeline' 
#   AND state = 'success'
# ORDER BY start_date DESC
# LIMIT 450;
# """
# df_tasks = pd.read_sql(query_tasks, engine)

# # Query DAG Run Duration (9.3.3)
# query_dag = """
# SELECT run_id, EXTRACT(EPOCH FROM (end_date - start_date)) as duration_seconds
# FROM dag_run
# WHERE dag_id = 'medallion_end_to_end_pipeline'
#   AND state = 'success'
# ORDER BY start_date DESC
# LIMIT 30;
# """
# df_dags = pd.read_sql(query_dag, engine)
# # Reverse to get chronological order (Run 1 to 30)
# df_dags = df_dags.iloc[::-1].reset_index(drop=True)
# df_dags['run_number'] = df_dags.index + 1

# def plot_task_latency(df_tasks):
#     # Calculate median duration for each task across the 30 runs
#     df_median = df_tasks.groupby('task_id')['duration'].median().reset_index()
#     df_median = df_median.sort_values(by='duration', ascending=False)

#     plt.figure(figsize=(12, 6))
#     ax = sns.barplot(x='duration', y='task_id', data=df_median, color='#4C72B0')
    
#     plt.title('Median Task Latency', pad=20, fontsize=14)
#     plt.xlabel('Duration (Seconds)')
#     plt.ylabel('Airflow Task ID')
    
#     # Add value labels to bars
#     for p in ax.patches:
#         ax.annotate(f"{p.get_width():.1f}s", 
#                     (p.get_width(), p.get_y() + p.get_height() / 2.), 
#                     ha='left', va='center', xytext=(5, 0), textcoords='offset points')
    
#     plt.tight_layout()
#     plt.savefig('task_latency_chart.png', dpi=300)
#     plt.show()

# plot_task_latency(df_tasks)


# def plot_dag_duration(df_dags):
#     # Convert seconds to minutes for better readability
#     df_dags['duration_minutes'] = df_dags['duration_seconds'] / 60.0

#     plt.figure(figsize=(10, 5))
#     plt.plot(df_dags['run_number'], df_dags['duration_minutes'], 
#              marker='o', linestyle='-', color='#C44E52', linewidth=2)
    
#     plt.title('End-to-End DAG Execution Time', pad=20, fontsize=14)
#     plt.xlabel('DAG Run Number')
#     plt.ylabel('Duration (Minutes)')
#     plt.ylim(0, df_dags['duration_minutes'].max() * 1.2) # Add headroom
    
#     plt.tight_layout()
#     plt.savefig('dag_duration_chart.png', dpi=300)
#     plt.show()

# plot_dag_duration(df_dags)

# def plot_resource_utilization(csv_file='work/docker_stats.csv'):
#     # Read the data
#     df = pd.read_csv(csv_file)
    
#     # Clean percentage signs and convert to float
#     df['CPU_Percent'] = df['CPU_Percent'].str.replace('%', '').astype(float)
    
#     # Parse memory (e.g., "2.5GiB / 16GiB" -> extract just the 2.5)
#     def parse_memory(mem_str):
#         val = mem_str.split(' / ')[0]
#         if 'GiB' in val: return float(val.replace('GiB', ''))
#         if 'MiB' in val: return float(val.replace('MiB', '')) / 1024
#         return 0.0
        
#     df['Mem_GiB'] = df['Memory_Usage'].apply(parse_memory)
    
#     # Aggregate sum across all containers per timestamp
#     df_agg = df.groupby('Timestamp').sum().reset_index()
    
#     fig, ax1 = plt.subplots(figsize=(12, 5))

#     # Plot CPU
#     color1 = '#4C72B0'
#     ax1.set_xlabel('Time (HH:MM:SS)')
#     ax1.set_ylabel('Total CPU Usage (%)', color=color1)
#     ax1.plot(df_agg['Timestamp'], df_agg['CPU_Percent'], color=color1, linewidth=2)
#     ax1.tick_params(axis='y', labelcolor=color1)
    
#     # Reduce x-axis clutter
#     ax1.set_xticks(ax1.get_xticks()[::len(ax1.get_xticks())//10]) 

#     # Plot Memory on secondary Y axis
#     ax2 = ax1.twinx()  
#     color2 = '#55A868'
#     ax2.set_ylabel('Total Memory Usage (GiB)', color=color2)
#     ax2.plot(df_agg['Timestamp'], df_agg['Mem_GiB'], color=color2, linewidth=2)
#     ax2.tick_params(axis='y', labelcolor=color2)

#     plt.title('Cluster Resource Utilization During Phase 2 (Parallel Silver Tasks)', pad=20)
#     fig.tight_layout()
#     plt.savefig('resource_utilization_chart.png', dpi=300)
#     plt.show()

# plot_resource_utilization('docker_stats.csv')

# import pandas as pd
# from delta.tables import DeltaTable

# # 1. Define your Silver tables and their S3 paths based on your provided scripts
# silver_tables = {
#     "silver_orders": "s3a://olist-data/silver/orders",
#     "silver_items": "s3a://olist-data/silver/order_items",
#     "silver_payments": "s3a://olist-data/silver/payments",
#     "silver_reviews": "s3a://olist-data/silver/reviews",
#     "silver_products": "s3a://olist-data/silver/products",
#     "silver_sellers": "s3a://olist-data/silver/sellers",
#     "silver_customers": "s3a://olist-data/silver/customers",
#     "silver_geolocation": "s3a://olist-data/silver/geolocation",
#     "silver_helpdesk_tickets": "s3a://olist-data/silver/helpdesk",
#     "silver_crm_identities": "s3a://olist-data/silver/crm"
# }

# # 2. Extract metrics from Delta Lake History <- this still error
# metrics_data = []

# print("Extracting Delta Lake History Metrics...")
# for task_id, path in silver_tables.items():
#     try:
#         dt = DeltaTable.forPath(spark, path)
        
#         # history(1) gets the absolute latest commit on the table
#         latest_commit = dt.history(1).collect()[0]
#         metrics = latest_commit.operationMetrics
        
#         # For MERGE operations, we want rows updated + rows inserted
#         # For OVERWRITE operations, we just want output rows
#         inserted = int(metrics.get("numTargetRowsInserted", 0))
#         updated = int(metrics.get("numTargetRowsUpdated", 0))
#         output_rows = int(metrics.get("numOutputRows", 0))
        
#         total_processed = inserted + updated if latest_commit.operation == "MERGE" else output_rows
        
#         metrics_data.append({
#             "task_id": task_id,
#             "operation": latest_commit.operation,
#             "records_processed": total_processed
#         })
#         print(f"✓ {task_id}: {total_processed} records ({latest_commit.operation})")
        
#     except Exception as e:
#          print(f"✗ Failed to extract {task_id}: {str(e)}")

# df_delta_metrics = pd.DataFrame(metrics_data)

# # 3. Merge Airflow Durations with Delta Records
# # (Assuming df_median is from your previous Airflow query)
# df_throughput = pd.merge(df_median, df_delta_metrics, on="task_id", how="inner")

# # 4. Calculate Throughput (Records per Second)
# df_throughput['throughput_rec_sec'] = df_throughput['records_processed'] / df_throughput['duration']

# # Sort for plotting
# df_throughput = df_throughput.sort_values(by='throughput_rec_sec', ascending=False)
# print("\n--- Final Throughput Metrics ---")
# print(df_throughput[['task_id', 'throughput_rec_sec']].to_string(index=False))


# >>> Extracting Delta Lake History Metrics...
# ✓ silver_orders: 96483 records (WRITE)
# ✓ silver_items: 5 records (MERGE)
# ✓ silver_payments: 103886 records (WRITE)
# ✓ silver_reviews: 99224 records (WRITE)
# ✓ silver_products: 32951 records (WRITE)
# ✓ silver_sellers: 3095 records (WRITE)
# ✓ silver_customers: 5 records (MERGE)
# ✓ silver_geolocation: 19618 records (WRITE)
# ✓ silver_helpdesk_tickets: 19219 records (WRITE)
# ✓ silver_crm_identities: 96096 records (WRITE)
# ---------------------------------------------------------------------------
# NameError                                 Traceback (most recent call last)
# Cell In[6], line 52
#      48 df_delta_metrics = pd.DataFrame(metrics_data)
#      50 # 3. Merge Airflow Durations with Delta Records
#      51 # (Assuming df_median is from your previous Airflow query)
# ---> 52 df_throughput = pd.merge(df_median, df_delta_metrics, on="task_id", how="inner")
#      54 # 4. Calculate Throughput (Records per Second)
#      55 df_throughput['throughput_rec_sec'] = df_throughput['records_processed'] / df_throughput['duration']

# NameError: name 'df_median' is not defined


































# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from datetime import datetime

# # 1. Define the schedule (Run every day at 2:00 AM)
# default_args = {
#     'owner': 'data_engineer',
#     'start_date': datetime(2026, 4, 20),
#     'retries': 1,
# }

# with DAG('daily_cdp_update', default_args=default_args, schedule_interval='0 2 * * *', catchup=False) as dag:
#     # BRONZE
#     run_bronze_ingestion = BashOperator(
#             task_id='bronze_ingestion',
#             bash_command='spark-submit --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,/opt/airflow/jars/delta-storage-3.1.0.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar /opt/airflow/scripts/bronze_check.py'
#         )

#     # SILVER
#     run_silver_customers = BashOperator(
#         task_id='silver_customers',
#         bash_command='spark-submit --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,/opt/airflow/jars/delta-storage-3.1.0.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar /opt/airflow/scripts/silver_customers.py'
#     )

#     run_silver_orders = BashOperator(
#         task_id='silver_orders',
#         bash_command='spark-submit --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,/opt/airflow/jars/delta-storage-3.1.0.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar /opt/airflow/scripts/silver_orders.py'
#     )

#     run_silver_items = BashOperator(
#         task_id='silver_items',
#         bash_command='spark-submit --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,/opt/airflow/jars/delta-storage-3.1.0.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar /opt/airflow/scripts/silver_items.py'
#     )

#     run_silver_reviews = BashOperator(
#         task_id='silver_reviews',
#         bash_command='spark-submit --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,/opt/airflow/jars/delta-storage-3.1.0.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar /opt/airflow/scripts/silver_reviews.py'
#     )

#     run_silver_products = BashOperator(
#         task_id='silver_products',
#         bash_command='spark-submit --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,/opt/airflow/jars/delta-storage-3.1.0.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar /opt/airflow/scripts/silver_products.py'
#     )
#     run_silver_crm_identities = BashOperator(
#         task_id='silver_crm_identities',
#         bash_command='spark-submit --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,/opt/airflow/jars/delta-storage-3.1.0.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar /opt/airflow/scripts/silver_crm_identities.py'
#     )
    
#     run_silver_geolocation = BashOperator(
#         task_id='silver_geolocation',
#         bash_command='spark-submit --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,/opt/airflow/jars/delta-storage-3.1.0.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar /opt/airflow/scripts/silver_geolocation.py'
#     )
    
#     run_silver_helpdesk_tickets = BashOperator(
#         task_id='silver_helpdesk_tickets',
#         bash_command='spark-submit --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,/opt/airflow/jars/delta-storage-3.1.0.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar /opt/airflow/scripts/silver_helpdesk_tickets.py'
#     )
    
#     run_silver_payments = BashOperator(
#         task_id='silver_payments',
#         bash_command='spark-submit --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,/opt/airflow/jars/delta-storage-3.1.0.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar /opt/airflow/scripts/silver_payments.py'
#     )
    
#     run_silver_sellers = BashOperator(
#         task_id='silver_sellers',
#         bash_command='spark-submit --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,/opt/airflow/jars/delta-storage-3.1.0.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar /opt/airflow/scripts/silver_sellers.py'
#     )

#     # GOLD
#     run_gold_customer_360 = BashOperator(
#         task_id='gold_customer_360',
#         bash_command='spark-submit --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,/opt/airflow/jars/delta-storage-3.1.0.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar /opt/airflow/scripts/gold_customer_360.py'
#     )

#     run_gold_fact_order_enriched = BashOperator(
#         task_id='fact_orders_enriched',
#         bash_command='spark-submit --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,/opt/airflow/jars/delta-storage-3.1.0.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar /opt/airflow/scripts/fact_orders_enriched.py'
#     )

#     run_gold_agg_sales = BashOperator(
#         task_id='agg_sales_by_state_category_month',
#         bash_command='spark-submit --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,/opt/airflow/jars/delta-storage-3.1.0.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar /opt/airflow/scripts/agg_sales_by_state_category_month.py'
#     )

#     run_gold_segment_rfm = BashOperator(
#         task_id='customer_segment_rfm',
#         bash_command='spark-submit --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,/opt/airflow/jars/delta-storage-3.1.0.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar /opt/airflow/scripts/customer_segment_rfm.py'
#     )

#     # END

#     # will run nodes in [] parrallel
#     silver_tasks = [run_silver_customers, run_silver_orders, run_silver_items, run_silver_reviews, run_silver_products, run_silver_crm_identities, run_silver_geolocation, run_silver_helpdesk_tickets, run_silver_payments, run_silver_sellers]
#     run_bronze_ingestion >> silver_tasks

#     # Foundation GOLD
#     silver_tasks >> run_gold_customer_360
#     silver_tasks >> run_gold_fact_order_enriched

#     run_gold_customer_360 >> run_gold_segment_rfm
#     run_gold_fact_order_enriched >> run_gold_agg_sales

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# 1. Define the schedule (Manual trigger for testing)
default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2026, 4, 20),
    'retries': 1,
}

# The shared spark-submit command prefix (to keep the code clean)
SPARK_SUBMIT_CMD = '''
    spark-submit \
    --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,\
/opt/airflow/jars/delta-storage-3.1.0.jar,\
/opt/airflow/jars/hadoop-aws-3.3.4.jar,\
/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar \
'''

with DAG(
    'bronze_and_silver_pipeline', 
    default_args=default_args, 
    schedule_interval=None,  # Manual trigger
    catchup=False
) as dag:

    # ==========================================
    # 1. BRONZE LAYER
    # ==========================================
    run_bronze_ingestion = BashOperator(
        task_id='bronze_ingestion',
        bash_command=f'{SPARK_SUBMIT_CMD} /opt/airflow/scripts/bronze_check.py'
    )

    # ==========================================
    # 2. SILVER LAYER
    # ==========================================
    run_silver_customers = BashOperator(
        task_id='silver_customers',
        bash_command=f'{SPARK_SUBMIT_CMD} /opt/airflow/scripts/silver_customers.py'
    )

    run_silver_orders = BashOperator(
        task_id='silver_orders',
        bash_command=f'{SPARK_SUBMIT_CMD} /opt/airflow/scripts/silver_orders.py'
    )

    run_silver_items = BashOperator(
        task_id='silver_items',
        bash_command=f'{SPARK_SUBMIT_CMD} /opt/airflow/scripts/silver_items.py'
    )

    run_silver_reviews = BashOperator(
        task_id='silver_reviews',
        bash_command=f'{SPARK_SUBMIT_CMD} /opt/airflow/scripts/silver_reviews.py'
    )

    run_silver_products = BashOperator(
        task_id='silver_products',
        bash_command=f'{SPARK_SUBMIT_CMD} /opt/airflow/scripts/silver_products.py'
    )
    
    run_silver_crm_identities = BashOperator(
        task_id='silver_crm_identities',
        bash_command=f'{SPARK_SUBMIT_CMD} /opt/airflow/scripts/silver_crm_identities.py'
    )
    
    run_silver_geolocation = BashOperator(
        task_id='silver_geolocation',
        bash_command=f'{SPARK_SUBMIT_CMD} /opt/airflow/scripts/silver_geolocation.py'
    )
    
    run_silver_helpdesk_tickets = BashOperator(
        task_id='silver_helpdesk_tickets',
        bash_command=f'{SPARK_SUBMIT_CMD} /opt/airflow/scripts/silver_helpdesk_tickets.py'
    )
    
    run_silver_payments = BashOperator(
        task_id='silver_payments',
        bash_command=f'{SPARK_SUBMIT_CMD} /opt/airflow/scripts/silver_payments.py'
    )
    
    run_silver_sellers = BashOperator(
        task_id='silver_sellers',
        bash_command=f'{SPARK_SUBMIT_CMD} /opt/airflow/scripts/silver_sellers.py'
    )

    # ==========================================
    # 3. SET DEPENDENCIES
    # ==========================================
    # Group all silver tasks into a list
    silver_tasks = [
        run_silver_customers, 
        run_silver_orders, 
        run_silver_items, 
        run_silver_reviews, 
        run_silver_products, 
        run_silver_crm_identities, 
        run_silver_geolocation, 
        run_silver_helpdesk_tickets, 
        run_silver_payments, 
        run_silver_sellers
    ]

    # Bronze must finish successfully before ANY Silver task starts.
    # The silver tasks will run in parallel once Bronze is done.
    run_bronze_ingestion >> silver_tasks
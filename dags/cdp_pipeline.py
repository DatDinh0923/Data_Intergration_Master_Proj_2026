from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# 1. Define the schedule (Run every day at 2:00 AM)
default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2026, 4, 20),
    'retries': 1,
}

with DAG('daily_cdp_update', default_args=default_args, schedule_interval='0 2 * * *', catchup=False) as dag:
    # # Work from home then uncomment this
    # run_silver_customers = BashOperator(
    #     task_id='silver_customers',
    #     bash_command='spark-submit --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 /opt/airflow/scripts/silver_olist_customers.py'
    # )

    # run_silver_orders = BashOperator(
    #     task_id='silver_orders',
    #     bash_command='spark-submit --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 /opt/airflow/scripts/silver_olist_orders.py'
    # )

    # run_gold_cdp = BashOperator(
    #     task_id='gold_customer_360',
    #     bash_command='spark-submit --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 /opt/airflow/scripts/gold_customer_360.py'
    # )
    
    # Work at the company then uncomment this part
    run_silver_customers = BashOperator(
        task_id='silver_customers',
        bash_command='spark-submit --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,/opt/airflow/jars/delta-storage-3.1.0.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar /opt/airflow/scripts/silver_customers.py'
    )

    run_silver_orders = BashOperator(
        task_id='silver_orders',
        bash_command='spark-submit --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,/opt/airflow/jars/delta-storage-3.1.0.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar /opt/airflow/scripts/silver_orders.py'
    )

    run_silver_items = BashOperator(
        task_id='silver_items',
        bash_command='spark-submit --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,/opt/airflow/jars/delta-storage-3.1.0.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar /opt/airflow/scripts/silver_items.py'
    )

    run_silver_reviews = BashOperator(
        task_id='silver_reviews',
        bash_command='spark-submit --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,/opt/airflow/jars/delta-storage-3.1.0.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar /opt/airflow/scripts/silver_reviews.py'
    )

    run_silver_products = BashOperator(
        task_id='silver_products',
        bash_command='spark-submit --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,/opt/airflow/jars/delta-storage-3.1.0.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar /opt/airflow/scripts/silver_products.py'
    )

    run_gold_customer_360 = BashOperator(
        task_id='gold_customer_360',
        bash_command='spark-submit --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,/opt/airflow/jars/delta-storage-3.1.0.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar /opt/airflow/scripts/gold_customer_360.py'
    )

    run_gold_fact_order_enriched = BashOperator(
        task_id='fact_orders_enriched',
        bash_command='spark-submit --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,/opt/airflow/jars/delta-storage-3.1.0.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar /opt/airflow/scripts/fact_orders_enriched.py'
    )

    run_gold_agg_sales = BashOperator(
        task_id='agg_sales_by_state_category_month',
        bash_command='spark-submit --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,/opt/airflow/jars/delta-storage-3.1.0.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar /opt/airflow/scripts/agg_sales_by_state_category_month.py'
    )

    run_gold_segment_rfm = BashOperator(
        task_id='customer_segment_rfm',
        bash_command='spark-submit --jars /opt/airflow/jars/delta-spark_2.12-3.1.0.jar,/opt/airflow/jars/delta-storage-3.1.0.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar /opt/airflow/scripts/customer_segment_rfm.py'
    )

    # END

    # will run nodes in [] parrallel 
    # [run_silver_customers, run_silver_orders, run_silver_items, run_silver_reviews, run_silver_products] >> run_gold_cdp
    # [run_silver_customers, run_silver_orders, run_silver_items, run_silver_reviews, run_silver_products] >> run_gold_customer_360, run_gold_fact_order_enriched, run_gold_agg_sales, run_gold_segment_rfm
    # run_silver_customers >> run_silver_orders >> run_silver_items >> run_silver_reviews >> run_silver_products >> run_gold_cdp

    silver_tasks = [run_silver_customers, run_silver_orders, run_silver_items, run_silver_reviews, run_silver_products]
    # Foundation GOLD
    silver_tasks >> run_gold_customer_360
    silver_tasks >> run_gold_fact_order_enriched
    # Dervied Gold tables
    run_gold_customer_360 >> run_gold_segment_rfm
    run_gold_fact_order_enriched >> run_gold_agg_sales
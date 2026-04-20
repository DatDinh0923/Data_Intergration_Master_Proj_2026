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
    
    run_silver_customers = BashOperator(
        task_id='silver_customers',
        bash_command='spark-submit --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 /opt/airflow/scripts/silver_olist_customers.py'
    )

    run_silver_orders = BashOperator(
        task_id='silver_orders',
        bash_command='spark-submit --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 /opt/airflow/scripts/silver_olist_orders.py'
    )

    run_gold_cdp = BashOperator(
        task_id='gold_customer_360',
        bash_command='spark-submit --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 /opt/airflow/scripts/gold_customer_360.py'
    )
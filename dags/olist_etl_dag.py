from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, '/opt/airflow/scripts')

from transform_olist_data import (
    transform_orders,
    transform_order_items,
    transform_reviews,
    transform_products
)

# Default arguments
default_args = {
    'owner': 'rick',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'olist-etl-pipeline',
    default_args=default_args,
    description='A DAG for processing Olist data',
    schedule='@daily',
    catchup=False,
    tags=['etl', 'gcp', 'bigquery', 'olist'],
)

task_transform_orders = PythonOperator(
    task_id='transform_orders',
    python_callable=transform_orders,
    dag=dag,
)

task_transform_order_items = PythonOperator(
    task_id='transform_order_items',
    python_callable=transform_order_items,
    dag=dag,
)

task_transform_reviews = PythonOperator(
    task_id='transform_reviews',
    python_callable=transform_reviews,
    dag=dag,
)

task_transform_products = PythonOperator(
    task_id='transform_products',
    python_callable=transform_products,
    dag=dag,
)


# Set task dependencies
# Orders must complete before order_items
# Reviews and products can run independently
task_transform_orders >> task_transform_order_items
task_transform_orders >> task_transform_reviews
task_transform_orders >> task_transform_products
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from helpers.generate_dummy import generate_customer, generate_product, generate_region
from helpers.insert_data import insert_data_customer, insert_data_product, insert_data_region

# Airflow DAG Definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="generate_dummy_customer_data_modular",
    default_args=default_args,
    description="Generate and insert one dummy customer data record into PostgreSQL",
    schedule_interval=timedelta(minutes=15),  # Every 20 seconds
    #schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),  # Set to a recent date
    catchup=False,
    max_active_runs=1,
) as dag:

    generate_data_task = PythonOperator(
        task_id="generate_customer",
        python_callable=generate_customer,
    )

    insert_data_customer_task = PythonOperator(
        task_id="insert_data_customer",
        python_callable=insert_data_customer,
    )
    generate_data_product_task = PythonOperator(
        task_id="generate_data_product",
        python_callable=generate_product, 
    )
    insert_data_product_task = PythonOperator(
        task_id="insert_data_product",
        python_callable=insert_data_product,
    )

    generate_data_region_task = PythonOperator(
        task_id="generate_data_region",
        python_callable=generate_region,
    )    
    
    insert_data_region_task = PythonOperator(
        task_id="insert_data_region",
        python_callable=insert_data_region,      
    )

    generate_data_task >> insert_data_customer_task  >> generate_data_product_task >> insert_data_product_task >> generate_data_region_task >> insert_data_region_task

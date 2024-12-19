from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from helpers.generate_dummy import (
    generate_customer, generate_product, generate_region, generate_sales_transactions
)
from helpers.insert_data import (
    insert_data_customer, insert_data_product, insert_data_region, insert_sales_transactions
)

#declaring DAG definitions
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="DAG1_generate_data_load_to_postgre",
    default_args=default_args,
    description="Generate and insert biodiesel data records into PostgreSQL",
    schedule_interval="*/5 * * * *",  # update schedule every 5 minutes
    start_date=datetime(2024, 12, 19),  
    catchup=False,
    max_active_runs=1,
) as dag:

    #declare group task for customer table
    with TaskGroup("customer_tasks", tooltip="Generate and insert customer data") as customer_group:
        generate_customer_task = PythonOperator(
            task_id="generate_customer",
            python_callable=generate_customer,
        )
        insert_customer_task = PythonOperator(
            task_id="insert_customer",
            python_callable=insert_data_customer,
        )
        generate_customer_task >> insert_customer_task

    #declare group task for product table
    with TaskGroup("product_tasks", tooltip="Generate and insert product data") as product_group:
        generate_product_task = PythonOperator(
            task_id="generate_product",
            python_callable=generate_product,
        )
        insert_product_task = PythonOperator(
            task_id="insert_product",
            python_callable=insert_data_product,
        )
        generate_product_task >> insert_product_task

    #declare group task for region table
    with TaskGroup("region_tasks", tooltip="Generate and insert region data") as region_group:
        generate_region_task = PythonOperator(
            task_id="generate_region",
            python_callable=generate_region,
        )
        insert_region_task = PythonOperator(
            task_id="insert_region",
            python_callable=insert_data_region,
        )
        generate_region_task >> insert_region_task

    #declare group task for sales_transactions table
    with TaskGroup("sales_transactions_tasks", tooltip="Generate and insert sales transactions data") as sales_group:
        generate_sales_task = PythonOperator(
            task_id="generate_sales",
            python_callable=generate_sales_transactions,
        )
        insert_sales_task = PythonOperator(
            task_id="insert_sales",
            python_callable=insert_sales_transactions,
        )
        generate_sales_task >> insert_sales_task

    #declare task dependencies for sequential process
    customer_group >> product_group >> region_group >> sales_group

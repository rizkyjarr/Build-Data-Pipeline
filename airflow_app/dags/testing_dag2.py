from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from helpers.generate_dummy import generate_dummy_data
from helpers.insert_data import insert_data

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
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    generate_data_task = PythonOperator(
        task_id="generate_dummy_data",
        python_callable=generate_dummy_data,
    )

    insert_data_task = PythonOperator(
        task_id="insert_dummy_data",
        python_callable=insert_data,
    )

    generate_data_task >> insert_data_task

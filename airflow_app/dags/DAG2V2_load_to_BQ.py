from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta, date
from google.cloud import bigquery
from decimal import Decimal
import psycopg2
import os
import pytz
from helpers.helper_bigquery import insert_incremental_data_to_bq, create_table_with_delay, replicate_table, create_sales_dashboard

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/credentials.json"
client = bigquery.Client()
local_tz = pytz.timezone('Asia/Jakarta')

BIGQUERY_PROJECT = "purwadika"
BIGQUERY_DATASET = "rizky_biodiesel_capstone3"

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    dag_id="DAG2_load_to_BQ_V2",
    default_args=default_args,
    schedule_interval="0 9 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    tables = [
        {
            "name": "sales_transactions",
            "bq_schema": [
                bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("sale_date", "DATE"),
                bigquery.SchemaField("product_id", "INTEGER"),
                bigquery.SchemaField("customer_id", "INTEGER"),
                bigquery.SchemaField("region_id", "INTEGER"),
                bigquery.SchemaField("quantity", "INTEGER"),
                bigquery.SchemaField("price", "NUMERIC"),
                bigquery.SchemaField("total_revenue", "NUMERIC"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
            ],
            "date_column": "created_at",
            "partition_field": "created_at",
            "unique_key": "id",
        },
        {
            "name": "customer",
            "bq_schema": [
                bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("sector", "STRING"),
                bigquery.SchemaField("type", "STRING"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
            ],
            "date_column": "created_at",
            "partition_field": "created_at",
            "unique_key": "id",
        },
        {
            "name": "product",
            "bq_schema": [
                bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("type", "STRING"),
                bigquery.SchemaField("price", "INTEGER"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
            ],
            "date_column": "created_at",
            "partition_field": "created_at",
            "unique_key": "id",
        },
        {
            "name": "region",
            "bq_schema": [
                bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("region_name", "STRING"),
                bigquery.SchemaField("province", "STRING"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
            ],
            "date_column": "created_at",
            "partition_field": "created_at",
            "unique_key": "id",
        },
    ]

    sales_dashboard_task = PythonOperator(
        task_id="generate_data_for_sales_dashboard",
        python_callable=create_sales_dashboard,
    )

    for table in tables:
        with TaskGroup(group_id=f"group_{table['name']}") as group:
            ensure_table_task = PythonOperator(
                task_id=f"Ensure_stg_{table['name']}",
                python_callable=create_table_with_delay,
                op_kwargs={
                    "table_name": table["name"],
                    "bq_schema": table["bq_schema"],
                },
            )

            extract_and_load_task = PythonOperator(
                task_id=f"extract_and_load_stg_{table['name']}",
                python_callable=insert_incremental_data_to_bq,
                op_kwargs={
                    "table_name": table["name"],
                    "db_schema": "public",
                    "date_column": table["date_column"],
                    "partition_field": table["partition_field"],
                    "h_minus": 1,
                },
            )

            final_table_task = PythonOperator(
                task_id=f"finalize_stg_{table['name']}",
                python_callable=replicate_table,
                op_kwargs={
                    "table_name": table["name"],
                    "unique_key": table["unique_key"],
                    "partition_field": table["partition_field"],
                },
            )

            # Explicitly define task dependencies for each table
            ensure_table_task >> extract_and_load_task >> final_table_task
                # Add dependency to sales_dashboard_task
            final_table_task >> sales_dashboard_task
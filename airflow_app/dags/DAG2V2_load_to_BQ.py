from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta, date
from google.cloud import bigquery
from decimal import Decimal
import psycopg2
import os
import pytz
from helpers.helper_bigquery import db_connection, table_exists, create_table_staging, serialize_value,extract_from_postgre, insert_incremental_data_to_bq, create_table_with_delay

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
    catchup=False
) as dag:
    
    tables = [
        {
            "name": "sales_transactions",
            # "db_schema" : "public",
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
            # "h_minus" : 1,
        },
        {
            "name": "customer",
            # "db_schema" : "public",
            "bq_schema": [
                bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("sector", "STRING"),
                bigquery.SchemaField("type", "STRING"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
            ],
            "date_column": "created_at",
            "partition_field": "created_at",
            # "h_minus" : 1,
        },
        {
            "name": "product",
            # "db_schema" : "public",
            "bq_schema": [
                bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("type", "STRING"),
                bigquery.SchemaField("price", "INTEGER"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
            ],
            "date_column": "created_at",
            "partition_field": "created_at",
            # "h_minus" : 1,
        },
        {
            "name": "region",
            # "db_schema" : "public",
            "bq_schema": [
                bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("region_name", "STRING"),
                bigquery.SchemaField("province", "STRING"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
            ],
            "date_column": "created_at",
            "partition_field": "created_at",
            # "h_minus" : 1,
        }
    ]

    for table in tables:
        with TaskGroup(group_id = f"group_{table['name']}") as group:
            ensure_table_task = PythonOperator(
                task_id = f"Ensure_stg_{table['name']}",
                python_callable=create_table_with_delay,
                op_kwargs={
                    "table_name": table['name'],
                    "bq_schema": table['bq_schema']
                },
            )
            
            extract_and_load_task = PythonOperator(
                task_id = f"extract_and_load_stg_{table['name']}",
                python_callable=insert_incremental_data_to_bq,
                op_kwargs={
                    "table_name" : table['name'],
                    "db_schema" :"public",
                    "date_column": "created_at",
                    "partition_field": table["partition_field"],
                    "h_minus": 1,

                },
        )

    ensure_table_task >>  extract_and_load_task

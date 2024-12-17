from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta, date
from google.cloud import bigquery
from decimal import Decimal
import psycopg2
import os

# Set Google Cloud Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\user\OneDrive\RFA _Personal Files\02. COURSE\Purwadhika_Data Engineering\Purwadhika_VS\capstone3_purwadhika\credentials.json"

# PostgreSQL Connection
def db_connection():
    return psycopg2.connect(
        host="host.docker.internal",
        database="postgres",
        user="de_admin",
        password="biodiesel"
    )

# BigQuery Configuration
BIGQUERY_PROJECT = "purwadika"
BIGQUERY_DATASET = "rizky_biodiesel_capstone3"

# PostgreSQL Connection
def db_connection():
    return psycopg2.connect(
        host="host.docker.internal",
        database="postgres",
        user="de_admin",
        password="biodiesel"
    )

# BigQuery Configuration
BIGQUERY_PROJECT = "purwadika"
BIGQUERY_DATASET = "rizky_biodiesel_capstone3"

def ensure_bigquery_table(table_name, schema, partition_field=None):
    client = bigquery.Client()
    table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{table_name}"
    try:
        client.get_table(table_id)
        print(f"Table {table_id} already exists.")
    except Exception:
        table = bigquery.Table(table_id, schema=schema)
        if partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field=partition_field
            )
        client.create_table(table)
        print(f"Table {table_id} created successfully.")

def extract_incremental_data(table_name, date_column, h_minus=1, **kwargs):
    target_date = (datetime.now() - timedelta(days=h_minus)).strftime("%Y-%m-%d")
    query = f"SELECT * FROM {table_name} WHERE DATE({date_column}) = '{target_date}'"

    conn = db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            print(f"Extracted {len(rows)} rows from {table_name}.")
            return {"columns": columns, "rows": rows}
    finally:
        conn.close()

def load_to_bigquery(table_name, schema, partition_field=None, **kwargs):
    ti = kwargs['ti']
    extracted_data = ti.xcom_pull(task_ids=f"extract_{table_name}")
    columns = extracted_data["columns"]
    rows = extracted_data["rows"]

    client = bigquery.Client()
    table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.stg_{table_name}"

    data_to_insert = []
    for row in rows:
        row_dict = dict(zip(columns, row))
        for key, value in row_dict.items():
            if isinstance(value, (datetime, date)):
                row_dict[key] = value.isoformat()
            elif isinstance(value, Decimal):
                row_dict[key] = float(value)
        data_to_insert.append(row_dict)

    errors = client.insert_rows_json(table_id, data_to_insert)
    if errors:
        print(f"Errors while inserting data into BigQuery: {errors}")
    else:
        print(f"Loaded {len(rows)} rows into {table_id}.")

# Airflow DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="postgres_to_bigquery_pipeline",
    default_args=default_args,
    schedule_interval="0 6 * * *",  # Runs daily at 6 AM
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    tables = [
        {
            "name": "sales_transactions",
            "schema": [
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
            "partition_field": "created_at"
        },
        {
            "name": "customer",
            "schema": [
                bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("sector", "STRING"),
                bigquery.SchemaField("type", "STRING"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
            ],
            "partition_field": "created_at"
        },
        {
            "name": "product",
            "schema": [
                bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("type", "STRING"),
                bigquery.SchemaField("price", "INTEGER"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
            ],
            "partition_field": "created_at"
        },
        {
            "name": "region",
            "schema": [
                bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("region_name", "STRING"),
                bigquery.SchemaField("province", "STRING"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
            ],
            "partition_field": "created_at"
        }
    ]

    for table in tables:
        with TaskGroup(group_id=f"group_{table['name']}") as group:
            ensure_task = PythonOperator(
                task_id=f"ensure_{table['name']}",
                python_callable=ensure_bigquery_table,
                op_kwargs={
                    "table_name": f"stg_{table['name']}",
                    "schema": table["schema"],
                    "partition_field": table["partition_field"],
                }
            )

            extract_task = PythonOperator(
                task_id=f"extract_{table['name']}",
                python_callable=extract_incremental_data,
                op_kwargs={
                    "table_name": table["name"],
                    "date_column": "created_at",
                    "h_minus": 1,
                }
            )

            load_task = PythonOperator(
                task_id=f"load_{table['name']}",
                python_callable=load_to_bigquery,
                op_kwargs={
                    "table_name": table["name"],
                    "schema": table["schema"],
                    "partition_field": table["partition_field"],
                }
            )

            # Define task dependencies
            ensure_task >> extract_task >> load_task
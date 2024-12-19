from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta, date
from google.cloud import bigquery
from decimal import Decimal
import psycopg2
import os
import pytz

#Setting up GCP credentials and timezone
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/credentials.json"
local_tz = pytz.timezone('Asia/Jakarta')

#Declare postgresql function that's containerized in docker
def db_connection():
    return psycopg2.connect(
        host="host.docker.internal",
        database="postgres",
        user="de_admin",
        password="biodiesel"
    )

#Declare table destination in bigquery
BIGQUERY_PROJECT = "purwadika"
BIGQUERY_DATASET = "rizky_biodiesel_capstone3"

#Ensuring the table, to check whether table exist or not, if not it creates new table with pre-defined nomenclature
def ensure_bigquery_table(table_name, schema, partition_field=None):
    client = bigquery.Client()
    table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{table_name}"

    try:
        # Check if the table exists
        client.get_table(table_id)
        print(f"Table {table_id} already exists.")
    except bigquery.NotFound:
        # Table does not exist, attempt to create it
        print(f"Table {table_id} does not exist. Attempting to create it.")
        table = bigquery.Table(table_id, schema=schema)
        if partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field=partition_field
            )
        try:
            client.create_table(table)
            print(f"Table {table_id} created successfully.")
        except bigquery.Conflict:
            # Table creation conflict (already exists due to concurrent operations)
            print(f"Table {table_id} already exists after retry.")
    except Exception as e:
        print(f"Error while ensuring table {table_id}: {e}")
        raise

def extract_and_load(table_name, schema, date_column, partition_field=None, h_minus=1):
    """Extracts data from PostgreSQL and loads it into BigQuery."""
    # Extract Incremental Data
    target_date = (datetime.now() - timedelta(days=h_minus)).strftime("%Y-%m-%d")
    query = f"SELECT * FROM {table_name} WHERE DATE({date_column}) = '{target_date}'"

    conn = db_connection()
    rows = []
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            if cursor.rowcount == 0:
                print(f"No rows found for table {table_name} on {target_date}.")
                return
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            print(f"Extracted {len(rows)} rows from {table_name}.")
    finally:
        conn.close()

    # Load Data into BigQuery
    if rows:
        client = bigquery.Client()
        table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.stg_{table_name}"

        # Convert rows into dictionaries and serialize dates/decimals
        data_to_insert = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            for key, value in row_dict.items():
                if isinstance(value, (datetime, date)):
                    row_dict[key] = value.isoformat()
                elif isinstance(value, Decimal):
                    row_dict[key] = float(value)
            data_to_insert.append(row_dict)

        # Insert rows into BigQuery
        errors = client.insert_rows_json(table_id, data_to_insert)
        if errors:
            print(f"Errors while inserting data into BigQuery: {errors}")
        else:
            print(f"Loaded {len(rows)} rows into {table_id}.")

# Setting up the DAG for the Airflow
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="DAG2_load_to_BQ",
    default_args=default_args,
    schedule_interval="0 9 * * *",  # This code intructs to update daily at 6 AM
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

            extract_and_load_task = PythonOperator(
                task_id=f"extract_and_load_{table['name']}",
                python_callable=extract_and_load,
                op_kwargs={
                    "table_name": table["name"],
                    "schema": table["schema"],
                    "date_column": "created_at",
                    "partition_field": table["partition_field"],
                    "h_minus": 3
                }
            )

            ensure_task >> extract_and_load_task
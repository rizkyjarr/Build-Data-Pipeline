import pandas as pd
import psycopg2
from google.cloud import bigquery
from datetime import datetime, timedelta, date
import os
from decimal import Decimal

# Set Google Cloud Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\user\OneDrive\RFA _Personal Files\02. COURSE\Purwadhika_Data Engineering\Purwadhika_VS\capstone3_purwadhika\airflow_app\credentials.json"

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
        print(f"Table {table_id} does not exist. Creating now...")
        table = bigquery.Table(table_id, schema=schema)
        if partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field=partition_field
            )
        client.create_table(table)
        print(f"Table {table_id} created successfully.")

def extract_incremental_data(table_name, date_column, h_minus=1):
    target_date = (datetime.now() - timedelta(days=h_minus)).strftime("%Y-%m-%d")

    query = f"""
        SELECT * 
        FROM {table_name}
        WHERE DATE({date_column}) = '{target_date}'
    """
    print(f"Extracting data from {table_name} for {target_date}...")

    conn = db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            print(f"Extracted {len(rows)} rows from {table_name}.")
            return columns, rows
    finally:
        conn.close()

def load_to_bigquery(table_name, columns, rows, schema, partition_field=None):
    client = bigquery.Client()
    table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.stg_{table_name}"

    # Convert rows into dictionaries and serialize dates/decimals
    data_to_insert = []
    for row in rows:
        row_dict = dict(zip(columns, row))
        # Convert data types for JSON compatibility
        for key, value in row_dict.items():
            if isinstance(value, (datetime, date)):
                row_dict[key] = value.isoformat()  # Convert dates to string
            elif isinstance(value, Decimal):
                row_dict[key] = float(value)  # Convert Decimal to float
        data_to_insert.append(row_dict)

    # BigQuery Insert
    errors = client.insert_rows_json(table_id, data_to_insert)
    if errors:
        print(f"Errors while inserting data into BigQuery: {errors}")
    else:
        print(f"Loaded {len(rows)} rows into {table_id}.")

def process_table(table_name, date_column, schema, partition_field=None, h_minus=1):
    #ensure_bigquery_table(f"stg_{table_name}", schema, partition_field)
    columns, rows = extract_incremental_data(table_name, date_column, h_minus)

    if rows:
        load_to_bigquery(table_name, columns, rows, schema, partition_field)
    else:
        print(f"No data to load for {table_name} on H-{h_minus}.")

if __name__ == "__main__":
    # Table schema
    sales_transactions_schema = [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("sale_date", "DATE"),
        bigquery.SchemaField("product_id", "INTEGER"),
        bigquery.SchemaField("customer_id", "INTEGER"),
        bigquery.SchemaField("region_id", "INTEGER"),
        bigquery.SchemaField("quantity", "INTEGER"),
        bigquery.SchemaField("price", "NUMERIC"),
        bigquery.SchemaField("total_revenue", "NUMERIC"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
    ]

    process_table(
        table_name="sales_transactions",
        date_column="created_at",
        schema=sales_transactions_schema,
        partition_field="created_at",
        h_minus=1
    )

    # Customer table schema
    customer_schema = [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("sector", "STRING"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
    ]

    process_table(
        table_name="customer",
        date_column="created_at",
        schema=customer_schema,
        partition_field="created_at",
        h_minus=1
    )
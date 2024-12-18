from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from google.cloud import bigquery
import os

# Set Google Cloud Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/credentials.json"

# BigQuery Configuration
BIGQUERY_PROJECT = "purwadika"
DATASET = "rizky_biodiesel_capstone3"

# Function to create the dataset if not exists
def ensure_dataset(dataset_name, location="asia-southeast2"):
    client = bigquery.Client()
    dataset_id = f"{BIGQUERY_PROJECT}.{dataset_name}"
    try:
        client.get_dataset(dataset_id)
        print(f"Dataset {dataset_id} already exists.")
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = location
        client.create_dataset(dataset, exists_ok=True)
        print(f"Dataset {dataset_id} created successfully.")

# Function to replicate staging tables
def replicate_table(staging_table, table_name):
    client = bigquery.Client()
    staging_table_id = f"{BIGQUERY_PROJECT}.{DATASET}.{staging_table}"
    table_id = f"{BIGQUERY_PROJECT}.{DATASET}.{table_name}"

    query = f"""
    CREATE OR REPLACE TABLE `{table_id}` AS
    SELECT * FROM `{staging_table_id}`
    """
    query_job = client.query(query)
    query_job.result()  # Wait for the query to finish
    print(f"Replicated {staging_table} to {table_name}")

# Function to create aggregated sales_dashboard table
def create_sales_dashboard():
    client = bigquery.Client()
    query = f"""
    CREATE OR REPLACE TABLE `{BIGQUERY_PROJECT}.{DATASET}.sales_dashboard` AS
    SELECT
        s.sale_date,
        c.name AS customer_name,
        c.type AS customer_type,
        r.region_name,
        p.name AS product_name,
        COUNT(s.id) AS transaction_count,
        SUM(s.quantity) AS total_quantity,
        SUM(s.total_revenue) AS total_revenue
    FROM
        `{BIGQUERY_PROJECT}.{DATASET}.sales_transactions` s
    JOIN
        `{BIGQUERY_PROJECT}.{DATASET}.region` r
        ON s.region_id = r.id
    JOIN
        `{BIGQUERY_PROJECT}.{DATASET}.product` p
        ON s.product_id = p.id
    JOIN
        `{BIGQUERY_PROJECT}.{DATASET}.customer` c
        ON s.customer_id = c.id
    GROUP BY
        s.sale_date, c.name, c.type, r.region_name, p.name;
    """
    query_job = client.query(query)
    query_job.result()  # Wait for the query to finish
    print("Aggregated table `sales_dashboard` created/updated successfully.")

# Airflow DAG Definition
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="DAG3_final_data",
    default_args=default_args,
    schedule_interval="0 6 * * *",  # Daily at 6 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Ensure Dataset Task
    ensure_dataset_task = PythonOperator(
        task_id="ensure_dataset",
        python_callable=ensure_dataset,
        op_kwargs={"dataset_name": DATASET, "location": "asia-southeast2"},
    )

    # Replication Task Group
    with TaskGroup(group_id="replication_tasks") as replication_tasks:
        tables = [
            {"staging": "stg_sales_transactions", "table_name": "sales_transactions"},
            {"staging": "stg_customer", "table_name": "customer"},
            {"staging": "stg_product", "table_name": "product"},
            {"staging": "stg_region", "table_name": "region"},
        ]

        for table in tables:
            PythonOperator(
                task_id=f"replicate_{table['table_name']}",
                python_callable=replicate_table,
                op_kwargs={
                    "staging_table": table["staging"],
                    "table_name": table["table_name"],
                },
            )

    # Aggregation Task
    aggregate_task = PythonOperator(
        task_id="create_sales_dashboard",
        python_callable=create_sales_dashboard,
    )

    # Define DAG dependencies
    ensure_dataset_task >> replication_tasks >> aggregate_task

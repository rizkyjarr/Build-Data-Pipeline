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
DATASET = "rizky_biodiesel_capstone3"  # Single dataset for staging and final

# Function to replicate staging table to final table with deduplication and partitioning
def replicate_table(staging_table, final_table, unique_key, partition_field):
    """Replicates data from a staging table to a final table with partitioning and deduplication."""
    client = bigquery.Client()
    staging_table_id = f"{BIGQUERY_PROJECT}.{DATASET}.{staging_table}"
    final_table_id = f"{BIGQUERY_PROJECT}.{DATASET}.{final_table}"

    query = f"""
    CREATE OR REPLACE TABLE `{final_table_id}`
    PARTITION BY DATE({partition_field})
    CLUSTER BY {unique_key} AS
    SELECT * EXCEPT(row_num)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY {unique_key} ORDER BY {partition_field} DESC) AS row_num
        FROM `{staging_table_id}`
    )
    WHERE row_num = 1;
    """
    query_job = client.query(query)
    query_job.result()  # Wait for the query to finish
    print(f"Replicated and deduplicated {staging_table} to {final_table} with partitioning.")

# Function to create the sales dashboard table
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
    print("Sales dashboard created successfully.")

# Airflow DAG Definition
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="DAG3_final_data",
    default_args=default_args,
    schedule_interval="0 10 * * *",  # Daily at 6 AM
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    tables = [
        {"staging": "stg_sales_transactions", "final": "sales_transactions", "unique_key": "id", "partition_field": "created_at"},
        {"staging": "stg_customer", "final": "customer", "unique_key": "id", "partition_field": "created_at"},
        {"staging": "stg_product", "final": "product", "unique_key": "id", "partition_field": "created_at"},
        {"staging": "stg_region", "final": "region", "unique_key": "id", "partition_field": "created_at"},
    ]

    with TaskGroup(group_id="replication_tasks") as replication_tasks:
        for table in tables:
            PythonOperator(
                task_id=f"replicate_{table['final']}",
                python_callable=replicate_table,
                op_kwargs={
                    "staging_table": table["staging"],
                    "final_table": table["final"],
                    "unique_key": table["unique_key"],
                    "partition_field": table["partition_field"],
                },
            )

    create_dashboard_task = PythonOperator(
        task_id="create_sales_dashboard",
        python_callable=create_sales_dashboard,
    )

    replication_tasks >> create_dashboard_task

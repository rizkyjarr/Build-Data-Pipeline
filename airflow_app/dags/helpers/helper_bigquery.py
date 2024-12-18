from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os
import psycopg2
from datetime import datetime, timedelta, date
import pandas as pd

# Set Google Cloud Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\user\OneDrive\RFA _Personal Files\02. COURSE\Purwadhika_Data Engineering\Purwadhika_VS\capstone3_purwadhika\airflow_app\credentials.json"
client = bigquery.Client()

BIGQUERY_PROJECT = "purwadika"
BIGQUERY_DATASET = "rizky_biodiesel_capstone3"

def db_connection():
    return psycopg2.connect(
        host="host.docker.internal",
        database="postgres",
        user="de_admin",
        password="biodiesel"
    )

def table_exists(table_name):
    table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.stg_{table_name}"

    try:
        client.get_table(table_id)
        # print("Table {} already exists".format(table_id))
        return True
    except NotFound:
        # print("Table {} is not found".format(table_id))
        return False
    
def create_table_staging(table_name, bq_schema):
    table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.stg_{table_name}"

    if table_exists(table_name) == True:
        print(f"Table stg_{table_name} already exists. No attempt to create table")
    else:
        print(f"Table stg_{table_name} does not exist, attempting to create")
        table = bigquery.Table(table_id,bq_schema)
        table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="created_at")
        table = client.create_table(table)

        print(f"Created table {table.project}.{table.dataset_id}.stg_{table.table_id}, "f"partitioned on column {table.time_partitioning.field}.")
        
def extract_from_postgre(table_name, db_schema, date_column, partition_field=None, h_minus=1):
   
    # Compute the target date by subtracting h_minus days from today's date
    target_date = (datetime.now() - timedelta(days=h_minus)).strftime("%Y-%m-%d")
    
    # Base query for selecting data based on the computed target_date
    query = f"SELECT * FROM {db_schema}.{table_name} WHERE DATE({date_column}) = '{target_date}'"
    
    # Add partition_field to query if it's provided
    if partition_field:
        query += f" AND {partition_field} IS NOT NULL"
    
    # Connect to your PostgreSQL database
    conn = db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(query)
        data = cursor.fetchall()
        
        if not data:
            print(f"No data found for {target_date}.")
            return pd.DataFrame() 

        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(data, columns=column_names)


        print(f"Table {table_name} has been succesfully extracted, {df}")
        return df

    except Exception as e:
        print(f"Error: {e}")
        return None

    finally:
        cursor.close()
        conn.close()

def load_extracted_df_to_bigquery(table_name, db_schema, date_column, partition_field=None, h_minus=1):

    table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.stg_{table_name}"
    df = extract_from_postgre(table_name, db_schema, date_column, partition_field=None, h_minus=1)

    try:
        job = client.load_table_from_dataframe(df, table_id, 
                                               job_config=bigquery.LoadJobConfig(
                                                   write_disposition="WRITE_APPEND"  # Ensures data is appended
                                               ))

        job.result()

        print(f"Data loaded succesfully into {table_id}")

    except Exception as e:
        print(f"error loading data into BigQuery: {e}")

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
            "partition_field": "created_at"
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
            "partition_field": "created_at"
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
            "partition_field": "created_at"
        },
        {
            "name": "region2",
            "bq_schema": [
                bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("region_name", "STRING"),
                bigquery.SchemaField("province", "STRING"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
            ],
            "partition_field": "created_at"
        }
    ]

# # Trial 1 table_exists --
# for table in tables:
#     table_name = table["name"]
#     table_existence = table_exists(table_name)
#     table_existence

# # Trial2 create_table_staging - single --
# table_name1 = "test2_region"
# bq_schema1 = [
#                 bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
#                 bigquery.SchemaField("region_name", "STRING"),
#                 bigquery.SchemaField("province", "STRING"),
#                 bigquery.SchemaField("created_at", "TIMESTAMP"),
#             ]

#Trial 2 create_table_staging - multiple --
# for table in tables:
#     table_name = table["name"]
#     bq_schema = table["schema"]
#     create_table_staging(table_name,bq_schema)

# create_table_staging(table_name1,bq_schema1)

# # Trial 3 extract_from_postgre --
# table_name = "sales_transactions"
# db_schema = "public"
# date_column = "created_at"  # Replace with your date column
# partition_field = "created_at"  # Optional partition field, can be left as None
# h_minus = 1  # Extract data for 1 day ago

# df = extract_from_postgre(table_name, db_schema, date_column, partition_field, h_minus)

# if df is not None and not df.empty:
#     print(df)
# else:
#     print("No data extracted.")

# Trial 4 load data_to_bigquery
table_name = "sales_transactions"
db_schema = "public"
bq_schema = [
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
date_column = "created_at"  # Replace with your date column
partition_field = "created_at"  # Optional partition field, can be left as None
h_minus = 1  # Extract data for 1 day ago

extract_from_postgre(table_name,db_schema, date_column, partition_field, h_minus)



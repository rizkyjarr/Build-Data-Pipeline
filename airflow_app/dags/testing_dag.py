from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import psycopg2
import pytz

local_tz = pytz.timezone('Asia/Jakarta')
def db_connection():
    return psycopg2.connect(
        host="host.docker.internal",
        database="postgres",
        user="de_admin",
        password="biodiesel"
    )

companies = [
    "PT. PTRO", "PT. NIL", "PT. ABM", "PT. GAX", "PT. EXX", "PT. ROMS", "PT. FREN",
    "PT. XLAX", "PT. CASN", "PT. KEM", "PT. RFA", "PT. RFK", "PT. JAGC", "PT. ACC", "PT. TAFM"
]

company_type = ["Spot", "Contract"]

def create_table_if_not_exists():
    conn = db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS customer (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    type VARCHAR(50),
                    created_at TIMESTAMP NOT NULL
                )
            """)
            conn.commit()
    finally:
        conn.close()

def count_existing_data():
    conn = db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM customer")
            return cur.fetchone()[0]
    finally:
        conn.close()

def generate_dummy_data():
    created_at = datetime.now().astimezone(local_tz)
    created_at_str = created_at.strftime("%Y-%m-%d %H:%M:%S")
    return {
        'name': random.choice(companies),
        'type': random.choice(company_type),
        'created_at': created_at_str
    }

def is_company_exists(company_name):
    conn = db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM customer WHERE name = %s", (company_name,))
            return cur.fetchone() is not None
    finally:
        conn.close()

def insert_data():
    conn = db_connection()
    try:
        create_table_if_not_exists()
        existing_count = count_existing_data()
        print(f"Existing rows in the table: {existing_count}")

        if existing_count >= 15:
            print("Table already has 15 or more records. Exiting.")
            return

        dummy_data = generate_dummy_data()
        if not is_company_exists(dummy_data['name']):
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO customer (name, type, created_at) VALUES (%s, %s, %s)",
                    (dummy_data['name'], dummy_data['type'], dummy_data['created_at'])
                )
                conn.commit()
                print(f"Inserted: {dummy_data}")
        else:
            print(f"Skipped (exists): {dummy_data['name']}")
    finally:
        conn.close()

# Airflow DAG Definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="generate_dummy_customer_data",
    default_args=default_args,
    description="Insert one dummy customer data record into PostgreSQL",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    insert_dummy_customer_data = PythonOperator(
        task_id="insert_dummy_customer_data",
        python_callable=insert_data,
    )

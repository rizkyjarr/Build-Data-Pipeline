from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from helpers.postgres_helper import execute_sql_file 
from airflow.utils.task_group import TaskGroup
import helpers.generate_data as data_gen
from airflow.decorators import task


# ✅ The correct path inside the Airflow container
SQL_FOLDER = "/opt/airflow/dags/sql"

# Ensure the directory exists before listing files
if not os.path.exists(SQL_FOLDER):
    print(f"🚨 WARNING: SQL directory does not exist: {SQL_FOLDER}")
    sql_files = []  # Prevent breaking DAG
else:
    sql_files = sorted([f for f in os.listdir(SQL_FOLDER) if f.endswith(".sql")])

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 1),
    "retries": 5,
    "retry_delay":timedelta(seconds=10)
}

# Define the DAG
with DAG(
    "DAG1_load_to_postgre",
    default_args=default_args,
    schedule_interval="@daily",  # Runs daily; adjust as needed
    catchup=False
) as dag:
    previous_task = None
    # Create a task for each SQL file
    for sql_file in sql_files:
        table_name = sql_file.replace("create_", "").replace(".sql", "")  # Extract table name
        sql_file_path = os.path.join(SQL_FOLDER, sql_file)

        with TaskGroup(group_id=f"{table_name}_data") as table_group:

            # **Step 1: Check & Create Table**
            create_table_task = PythonOperator(
                task_id=f"check_if_{table_name}_exists",
                python_callable=execute_sql_file,
                op_args=[sql_file_path, table_name],
                depends_on_past=True
            )

            # **Step 2: Generate & Insert Data**
            generate_func = getattr(data_gen, f"generate_{table_name}", None)  # Get the function dynamically

            if generate_func:  # Only proceed if function exists

                @task(task_id=f"generate_and_insert_{table_name}")
                def generate_and_insert_data():
                    data = generate_func()  # Generate **only one** record
                    data_gen.insert_data(table_name, data)  # Insert into DB

                insert_task = generate_and_insert_data()

                # Set the dependency within the TaskGroup
                create_table_task >> insert_task

                # Set the dependency between TaskGroups (if there's a previous task)
                if previous_task:
                    previous_task >> create_table_task

                # Update the previous_task to the last task in the current TaskGroup
                previous_task = insert_task
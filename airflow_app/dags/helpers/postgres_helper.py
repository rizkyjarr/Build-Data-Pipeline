import psycopg2
from dotenv import load_dotenv
import os

# Load environment for DB configuration
load_dotenv()


# DB connection configuration
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

# Check whether specific table exist in the DB
def table_exists(table_name):
    query = f"""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = '{table_name}'
    );
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute(query)
        exists=cur.fetchone()[0]
        cur.close()
        conn.close()
        return exists
    except Exception as e:
        print("Error checking table existence: {e}")
        return False

# Execute SQL file
def execute_sql_file(sql_file_path, table_name):
    """Executes an SQL file if the table does not exist."""
    if table_exists(table_name):
        print(f"Table: {table_name} already exists, skipping execution")
        return

    conn = None
    cur = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        with open(sql_file_path, 'r') as file:
            sql_commands = file.read()

        cur.execute(sql_commands)
        conn.commit()

        print(f"SQL file executed and {table_name} table has been created")
    except Exception as e:
        print(f"Error executing SQL File: {e}")
        raise Exception(f"SQL execution failed: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# try function

# customer_path = r"C:\Users\user\OneDrive\RFA _Personal Files\02. COURSE\Purwadhika_Data Engineering\Purwadhika_VS\final_project\airflow_app\dags\include\sql\create_customer.sql"
# driver_path = r"C:\Users\user\OneDrive\RFA _Personal Files\02. COURSE\Purwadhika_Data Engineering\Purwadhika_VS\final_project\airflow_app\dags\include\sql\create_driver.sql"
# vehicle_path = r"C:\Users\user\OneDrive\RFA _Personal Files\02. COURSE\Purwadhika_Data Engineering\Purwadhika_VS\final_project\airflow_app\dags\include\sql\create_vehicle.sql"
# ride_path = r"C:\Users\user\OneDrive\RFA _Personal Files\02. COURSE\Purwadhika_Data Engineering\Purwadhika_VS\final_project\airflow_app\dags\include\sql\create_ride.sql"

# def main():
#     table_exists("customer")
#     execute_sql_file(customer_path, "customer")

#     table_exists("driver")
#     execute_sql_file(driver_path,"driver")

#     table_exists("vehicle")
#     execute_sql_file(vehicle_path,"vehicle")

#     table_exists("ride")
#     execute_sql_file(ride_path,"ride")

# if __name__ == "__main__":
#     main()
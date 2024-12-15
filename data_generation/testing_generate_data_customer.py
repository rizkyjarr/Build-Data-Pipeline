import random
import psycopg2
from datetime import datetime
import pytz


local_tz = pytz.timezone('Asia/Jakarta')
created_at = datetime.now().astimezone(local_tz)
created_at_str = created_at.strftime("%Y-%m-%d %H:%M:%S")

# Database connection setup
def db_connection():
    return psycopg2.connect(
        host="localhost",
        database="postgres",
        user="de_admin",
        password="biodiesel"
    )

companies = [
    "PT. PTRO", "PT. NIL", "PT. ABM", "PT. GAX", "PT. EXX", "PT. ROMS", "PT. FREN",
    "PT. XLAX", "PT. CASN", "PT. KEM", "PT. RFA", "PT. RFK", "PT. JAGC", "PT. ACC", "PT. TAFM"
]

company_type = ["Spot", "Contract"]

def create_table_if_not_exists(conn):
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

def count_existing_data(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM customer")
        return cur.fetchone()[0]

def generate_dummy_data():
    return {
        'name': random.choice(companies),
        'type': random.choice(company_type),
        'created_at': created_at_str
    }

def is_company_exists(conn, company_name):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM customer WHERE name = %s", (company_name,))
        return cur.fetchone() is not None
    
# Insert to database
def insert_data(conn, data):
    with conn.cursor() as cur:
        cur.execute("INSERT INTO customer (name, type, created_at) VALUES (%s, %s, %s)",
            (data['name'], data['type'], data['created_at'])

        )
        conn.commit()

# Main function
def main():
    conn = db_connection()
    try:
        create_table_if_not_exists(conn)  # Ensure the table is created
        existing_count = count_existing_data(conn)
        print(f"Existing rows in the table: {existing_count}")

        if existing_count >= 15:
            print("Table already has 15 or more records. Exiting.")
            return

        listed_companies = set()
        while len(listed_companies) < 15 - existing_count:
            dummy_data = generate_dummy_data()
            if dummy_data['name'] in listed_companies:
                continue  # Skip if company is already listed
            if not is_company_exists(conn, dummy_data['name']):
                insert_data(conn, dummy_data)
                listed_companies.add(dummy_data['name'])
                print(f"Inserted: {dummy_data}")
            else:
                print(f"Skipped (exists): {dummy_data['name']}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()

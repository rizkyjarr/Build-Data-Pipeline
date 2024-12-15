import psycopg2
from helpers.generate_dummy import generate_dummy_data

def db_connection():
    return psycopg2.connect(
        host="host.docker.internal",
        database="postgres",
        user="de_admin",
        password="biodiesel"
    )

def is_company_exists(conn, company_name):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM customer WHERE name = %s", (company_name,))
        return cur.fetchone() is not None

def insert_data():
    conn = db_connection()
    try:
        with conn.cursor() as cur:
            # Ensure the table exists
            cur.execute("""
                CREATE TABLE IF NOT EXISTS customer (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    type VARCHAR(50),
                    created_at TIMESTAMP NOT NULL
                )
            """)
            conn.commit()

            # Generate new dummy data
            for _ in range(10):  # Attempt 10 times to find unique data
                dummy_data = generate_dummy_data()
                if not is_company_exists(conn, dummy_data['name']):
                    cur.execute(
                        "INSERT INTO customer (name, type, created_at) VALUES (%s, %s, %s)",
                        (dummy_data['name'], dummy_data['type'], dummy_data['created_at'])
                    )
                    conn.commit()
                    print(f"Inserted: {dummy_data}")
                    return
                print(f"Skipped (exists): {dummy_data['name']}")

            print("Could not find unique data to insert after 10 attempts.")
    finally:
        conn.close()

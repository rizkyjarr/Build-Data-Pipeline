import psycopg2
from helpers.generate_dummy import generate_customer, generate_product, generate_region

def db_connection():
    return psycopg2.connect(
        host="host.docker.internal",
        database="postgres",
        user="de_admin",
        password="biodiesel"
    )

def is_data_exist_customer(conn, data_column):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM customer WHERE name = %s", (data_column,))
        return cur.fetchone() is not None
    
def is_data_exist_product(conn, data_column):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM product WHERE name = %s", (data_column,))
        return cur.fetchone() is not None

def is_data_exist_region(conn, region_name, province):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM region WHERE region_name = %s AND province = %s
        """, (region_name, province))
        return cur.fetchone() is not None
        
def insert_data_customer():
    conn = db_connection()
    try:
        with conn.cursor() as cur:
            # Ensure the table exists
            cur.execute("""
                CREATE TABLE IF NOT EXISTS customer (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    sector VARCHAR(50), 
                    type VARCHAR(50),
                    created_at TIMESTAMP NOT NULL
                )
            """)
            conn.commit()

            # Generate new dummy data
            for _ in range(10):  # Attempt 10 times to find unique data
                customer_data = generate_customer()
                if not is_data_exist_customer(conn, customer_data['name']):
                    cur.execute(
                        "INSERT INTO customer (name, sector, type, created_at) VALUES (%s, %s, %s, %s)",
                        (customer_data['name'],customer_data['sector'], customer_data['type'], customer_data['created_at'])
                    )
                    conn.commit()
                    print(f"Inserted: {customer_data}")
                    return
                print(f"Skipped (exists): {customer_data['name']}")

            print("Data is not found in the db after 10 attempts")
    finally:
        conn.close()

def insert_data_product():
    conn = db_connection()
    try:
        with conn.cursor() as cur:
            # Ensure the table exists
            cur.execute("""
                CREATE TABLE IF NOT EXISTS product (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    type VARCHAR(50),
                    price INTEGER,
                    created_at TIMESTAMP NOT NULL
                )
            """)
            conn.commit()

            # Generate new dummy data
            for _ in range(10):  # Attempt 10 times to find unique data
                product_data = generate_product()
                if not is_data_exist_product(conn, product_data['name']):
                    cur.execute(
                        "INSERT INTO product (name, type, price, created_at) VALUES (%s, %s, %s, %s)",
                        (product_data['name'], product_data['type'], product_data['price'],product_data['created_at'])
                    )
                    conn.commit()
                    print(f"Inserted: {product_data}")
                    return
                print(f"Skipped (exists): {product_data['name']}")

            print("Data is not find in the db after 10 attempts")
    finally:
        conn.close()

def insert_data_region():
    conn = db_connection()
    try:
        with conn.cursor() as cur:
            # Ensure the table exists
            cur.execute("""
                CREATE TABLE IF NOT EXISTS region (
                    id SERIAL PRIMARY KEY,
                    region_name VARCHAR(255) NOT NULL,
                    province VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP NOT NULL
                )
            """)
            conn.commit()

            # Generate new dummy data
            for _ in range(10):  # Attempt 10 times to find unique data
                region_data = generate_region()
                if not is_data_exist_region(conn, region_data['region_name'], region_data['province']):
                    cur.execute(
                        "INSERT INTO region (region_name, province, created_at) VALUES (%s, %s, %s)",
                        (region_data['region_name'], region_data['province'], region_data['created_at'])
                    )
                    conn.commit()
                    print(f"Inserted: {region_data}")
                    return
                print(f"Skipped (exists): {region_data['region_name']}")

            print("Data is not find in the db after 10 attempts")
    finally:
        conn.close()
import psycopg2
from helpers.generate_dummy import generate_customer, generate_product, generate_region, generate_sales_transactions

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


def insert_sales_transactions():
    conn = db_connection()
    try:
        with conn.cursor() as cur:
            # Ensure the table exists
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sales_transactions (
                    id SERIAL PRIMARY KEY,
                    sale_date DATE NOT NULL,
                    product_id INTEGER NOT NULL,
                    customer_id INTEGER NOT NULL,
                    region_id INTEGER NOT NULL,
                    quantity INTEGER NOT NULL,
                    price NUMERIC NOT NULL,
                    total_revenue NUMERIC NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    FOREIGN KEY (product_id) REFERENCES product(id),
                    FOREIGN KEY (customer_id) REFERENCES customer(id),
                    FOREIGN KEY (region_id) REFERENCES region(id)
                )
            """)
            conn.commit()
            print("Sales fact table has been created or verified.")

        # Generate sales data
        sales_transactions_data = generate_sales_transactions(conn)

        # Insert the data
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO sales_transactions (
                    sale_date, product_id, customer_id, region_id, quantity, price, total_revenue, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                sales_transactions_data['sale_date'],
                sales_transactions_data['product_id'],
                sales_transactions_data['customer_id'],
                sales_transactions_data['region_id'],
                sales_transactions_data['quantity'],
                sales_transactions_data['price'],
                sales_transactions_data['total_revenue'],
                sales_transactions_data['created_at']
            ))
            conn.commit()
            print("Sales transactions data has been inserted into the table.")
    finally:
        conn.close()
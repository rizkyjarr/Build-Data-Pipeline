import random
from datetime import datetime
import pytz
import psycopg2

local_tz = pytz.timezone('Asia/Jakarta')
created_at = datetime.now().astimezone(local_tz)
created_at_str = created_at.strftime("%Y-%m-%d %H:%M:%S")

companies_list = [
    "PT. PTRO", "PT. NIL", "PT. ABM", "PT. GAX", "PT. EXX", "PT. ROMS", "PT. FREN",
    "PT. XLAX", "PT. CASN", "PT. KEM", "PT. RFA", "PT. RFK", "PT. JAGC", "PT. ACC", "PT. TAFM"
]
company_type = ["Spot", "Contract"]
company_sector = ["Coal", "Nickel"]
product_list = ["B0", "B35", "B40", "B50", "B100"]
product_type = ["Biodiesel"]

region_and_provinces = {
    "Kalimantan": ["North Kalimantan", "South Kalimantan", "Central Kalimantan", "East Kalimantan"],
    "Sulawesi": ["South Sulawesi", "South East Sulawesi"],
    "East Indonesia": ["Papua", "East Nusa Tenggara"],
}



#declare function to create connection with postgreDB
def db_connection():
    return psycopg2.connect(
        host="host.docker.internal",
        database="postgres",
        user="de_admin",
        password="biodiesel"
    )


#declare function to generate customer data
def generate_customer():
    customer_data = {
        'name': random.choice(companies_list), #random within predefined choice, in B2B business, customers are in small numbers
        'sector': random.choice(company_sector),
        'type': random.choice(company_type),
        'created_at': created_at_str
    }
    print(f"Customer data has been generated: {customer_data}")
    return customer_data


#declare function to generate customer data
def generate_product():
    product_data = {
        'name': random.choice(product_list), #random within predefined choice, in B2B business, products are in small numbers
        'type': random.choice(product_type),
        'price': random.randint(9800,10000),
        'created_at': created_at_str
    }
    print(f"Product data has been generated: {product_data}")
    return product_data



#declare function to generate region sales data
def generate_region():
    region_name = random.choice(list(region_and_provinces.keys())) #random within predefined dictionaries, in B2B business, regions are in small numbers
    province = random.choice(region_and_provinces[region_name])
    
    region_data = {
        "region_name": region_name,
        "province": province,
        "created_at": created_at_str
    }
    print(f"Region data has been generated: {region_data}")
    return region_data



#declare function to generate region sales data
def generate_sales_transactions(conn):
    conn = db_connection()
    
    
    with conn.cursor() as cur:
       
        #product_id is fetched from product table
        cur.execute("SELECT id, price FROM product ORDER BY RANDOM() LIMIT 1")
        product = cur.fetchone()
        if not product:
            raise ValueError("No products found in product table.")
        product_id, price = product

        #customer_id column is fetched from customer table
        cur.execute("SELECT id FROM customer ORDER BY RANDOM() LIMIT 1")
        customer = cur.fetchone()
        if not customer:
            raise ValueError("No customers found in customer table.")
        customer_id = customer[0]

        #region_id column is fetched from region table
        cur.execute("SELECT id FROM region ORDER BY RANDOM() LIMIT 1")
        region = cur.fetchone()
        if not region:
            raise ValueError("No regions found in region table.")
        region_id = region[0]

        #declare parameters for sales_transactions data
        quantity = random.randint(5000, 10000)  # Random quantity in liters
        total_revenue = quantity * price  # Calculate total revenue
        created_at_str

        sales_transactions_data = {
            "sale_date": datetime.now().astimezone(local_tz).strftime("%Y-%m-%d"),
            "product_id": product_id,
            "customer_id": customer_id,
            "region_id": region_id,
            "quantity": quantity,
            "price": price,
            "total_revenue": total_revenue,
            "created_at": created_at_str
        }

        print(f"Sales transactions data has been generated: {sales_transactions_data}")
        return sales_transactions_data
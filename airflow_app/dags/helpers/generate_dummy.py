import random
from datetime import datetime
import pytz

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

def generate_customer():
    customer_data = {
        'name': random.choice(companies_list),
        'sector': random.choice(company_sector),
        'type': random.choice(company_type),
        'created_at': created_at_str
    }
    print(f"Customer data has been generated: {customer_data}")
    return customer_data

def generate_product():
    product_data = {
        'name': random.choice(product_list),
        'type': random.choice(product_type),
        'price': random.randint(9800,10000),
        'created_at': created_at_str
    }
    print(f"Product data has been generated: {product_data}")
    return product_data
    
def generate_region():
    region_name = random.choice(list(region_and_provinces.keys()))
    province = random.choice(region_and_provinces[region_name])
    
    region_data = {
        "region_name": region_name,
        "province": province,
        "created_at": created_at_str
    }
    print(f"Region data has been generated: {region_data}")
    return region_data
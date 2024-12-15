import random
from datetime import datetime
import pytz

local_tz = pytz.timezone('Asia/Jakarta')
companies = [
    "PT. PTRO", "PT. NIL", "PT. ABM", "PT. GAX", "PT. EXX", "PT. ROMS", "PT. FREN",
    "PT. XLAX", "PT. CASN", "PT. KEM", "PT. RFA", "PT. RFK", "PT. JAGC", "PT. ACC", "PT. TAFM"
]
company_type = ["Spot", "Contract"]

def generate_dummy_data():
    created_at = datetime.now().astimezone(local_tz)
    created_at_str = created_at.strftime("%Y-%m-%d %H:%M:%S")
    dummy_data = {
        'name': random.choice(companies),
        'type': random.choice(company_type),
        'created_at': created_at_str
    }
    print(f"Generated dummy data: {dummy_data}")
    return dummy_data

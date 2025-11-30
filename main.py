"""
Program used to connect to supabase to upload data and create tables
CSC325 - Database Systems
Jean LaFrance and Tyler Rinko
11/30/25
"""


import os
from dotenv import load_dotenv
from utils.db_functions import getconn, insert_brands, insert_cpus, insert_gpus, insert_products, read_data, setup_db

DATA_LIMIT = 100  # Set to None to load all data, or set to a number (e.g., 100) to limit rows

# Get file from .env filepath
load_dotenv()
CSV_FILE = os.getenv("CSV_FILE_PATH")
if not CSV_FILE or not os.path.exists(CSV_FILE):
    raise FileNotFoundError(f"Could not find the CSV file at: {CSV_FILE}")

# Get db connection
print("Connecting to Supabase...")
cnx = getconn() 
cur = cnx.cursor()

# Setup db with appropriate tables and commit
setup_db(cur)
cnx.commit()

# Read CSV file once
brands_set, cpus_set, gpus_set, product_rows = read_data(CSV_FILE)

# Insert data and commit after each table
insert_brands(cur, brands_set)
cnx.commit() 
insert_cpus(cur, cpus_set)
cnx.commit() 
insert_gpus(cur, gpus_set)
cnx.commit() 
insert_products(cur, product_rows)
cnx.commit()

print("\nDatabase population complete")
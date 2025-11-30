"""
Program used to connect to supabase to upload data and create tables from queried data
CSC325 - Database Systems
Jean LaFrance and Tyler Rinko
11/30/25
"""


import os
import argparse
from dotenv import load_dotenv
from utils.chart_functions import show_avg_price_by_brand, show_avg_price_grouped, show_box_price_by_type, show_price_histogram, show_price_vs_cpu_tier
from utils.db_functions import getconn, insert_brands, insert_cpus, insert_gpus, insert_products, read_data, setup_db

def main():
    # Program chart arg options
    options = ["all", "price", "grouped-price", "boxed-price", "frequency-price", "tier-price"]

    # Command line arg parser
    parser = argparse.ArgumentParser(
        description='Upload data to a database, and create charts!'
    )
    parser.add_argument(
        "--chart",
        choices=options,
        help=f"Generate a specific chart. Options: {' '.join(options)}"
    )
    args = parser.parse_args()

    # Handle chart args
    if args.chart:
        print(f"Generating chart: {args.chart}...")

        """------- TODO: Query data to pass to chart function --------"""
        df="Fix me"

        if args.chart == "all":
            show_avg_price_by_brand(df)
            show_avg_price_grouped(df)
            show_box_price_by_type(df)
            show_price_histogram(df)
            show_price_vs_cpu_tier(df)
    
        elif args.chart == "price":
            show_avg_price_by_brand(df)

        elif args.chart == "grouped-price":
            show_avg_price_grouped(df)

        elif args.chart == "boxed-price":
            show_box_price_by_type(df)

        elif args.chart == "frequency-price":
            show_price_histogram(df)

        elif args.chart == "tier-price":
            show_price_vs_cpu_tier(df)

        return

    # Default
    create_DB()

# Create and setup database
def create_DB():
    DATA_LIMIT = 100  # Set to None to load all data

    # Get file from .env filepath
    load_dotenv()
    CSV_FILE = os.getenv("CSV_FILE_PATH")
    if not CSV_FILE or not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f"Could not find the CSV file at: {CSV_FILE}")

    DB_URL = os.getenv("DATABASE_URL")
    if not DB_URL:
        raise FileNotFoundError(f"DATABASE_URL not found in .env")

    # Get db connection
    print("Connecting to Supabase...")
    cnx = getconn(DB_URL) 
    cur = cnx.cursor()

    # Setup db with appropriate tables and commit
    setup_db(cur)
    cnx.commit()

    # Read CSV file once
    brands_set, cpus_set, gpus_set, product_rows = read_data(CSV_FILE, DATA_LIMIT)

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

if __name__ == "__main__":
    main()
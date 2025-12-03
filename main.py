import os
import sys
import db_functions as db
import chart_functions as charts

DB_NAME = "computers.db"
CSV_PATH = "computer_prices_all.csv" 

# Load csv data into database
def import_data():
    if not os.path.exists(CSV_PATH):
        print(f"Error: The file '{CSV_PATH}' was not found.")
        return False

    print(f"Connecting to local SQLite database: {DB_NAME}...")

    try:
        conn = db.getconn(DB_NAME)
        cur = conn.cursor()
    except Exception as e:
        print(f"Connection failed: {e}")
        return False

    try:
        # Create table schemata
        db.setup_db(cur)
        conn.commit()

        data_limit = None
        
        # Read in table data from the csv
        brands, cpus, gpus, products = db.read_data(CSV_PATH, data_limit)

        # Insert csv data into tables
        db.insert_brands(cur, brands)
        conn.commit()
        brand_map = db.get_brand_map(cur)
        db.insert_cpus(cur, cpus, brand_map)
        db.insert_gpus(cur, gpus, brand_map)
        db.insert_products(cur, products, brand_map)
        conn.commit()
        print("SUCCESS: All data imported into 'computers.db'")

        # Query the counts from all tables for display
        brands_count = cur.execute("SELECT COUNT(*) FROM Brands").fetchone()[0]
        cpus_count = cur.execute("SELECT COUNT(*) FROM CPU").fetchone()[0]
        gpus_count = cur.execute("SELECT COUNT(*) FROM GPU").fetchone()[0]
        products_count = cur.execute("SELECT COUNT(*) FROM Products").fetchone()[0]
        print(f"  Brands:   {brands_count}")
        print(f"  CPUs:     {cpus_count}")
        print(f"  GPUs:     {gpus_count}")
        print(f"  Products: {products_count}")
        
        return True

    except Exception as e:
        print(f"\nCRITICAL ERROR: {e}")
        conn.rollback()
        return False
    
    finally:
        cur.close()
        conn.close()

# Show all table visualizations on one page
def show_dashboard():
    if not os.path.exists(DB_NAME):
        print(f"Error: Database '{DB_NAME}' not found. Please import data first.")
        return

    print(f"Connecting to database: {DB_NAME}...")
    
    try:
        conn = db.getconn(DB_NAME)
        
        print("Loading data from database...")
        df = charts.get_products_dataframe(conn)
        print(f"Loaded {len(df)} products.\n")
        
        print("Generating dashboard...")
        charts.show_dashboard(df)
        
        print("\nDashboard displayed in your browser!")
        
    except Exception as e:
        print(f"Error generating dashboard: {e}")
    
    finally:
        conn.close()

# Display each table visualization on its own page
def show_visualizations():
    if not os.path.exists(DB_NAME):
        print(f"Error: Database '{DB_NAME}' not found. Please import data first.")
        return

    print(f"Connecting to database: {DB_NAME}...")
    
    try:
        conn = db.getconn(DB_NAME)
        
        print("Loading data from database...")
        df = charts.get_products_dataframe(conn)
        print(f"Loaded {len(df)} products.\n")
        
        print("Generating visualizations...\n")
        
        print("1. Price Distribution Histogram")
        charts.show_price_histogram(df)
        
        print("2. Average Price by Brand")
        charts.show_avg_price_by_brand(df)
        
        print("3. Average Price Grouped by Type")
        charts.show_avg_price_grouped(df)
        
        print("4. Price vs CPU Tier")
        charts.show_price_vs_cpu_tier(df)
        
        print("5. Price Distribution: Laptop vs Desktop")
        charts.show_box_price_by_type(df)
        
        print("\nAll visualizations displayed!")
        
    except Exception as e:
        print(f"Error generating visualizations: {e}")
    
    finally:
        conn.close()

# Main menu selection
def show_menu():
    print("Computer Price Analysis")
    print("1. Import data from CSV")
    print("2. Show dashboard (all charts in one window)")
    print("3. Show all visualizations (separate windows)")
    print("4. Show specific chart")
    print("5. Exit")
    
    choice = input("\nEnter your choice (1-5): ").strip()
    return choice

# Individual chart selection
def show_chart_menu():
    print("\nAvailable Charts:")
    print("1. Price Distribution Histogram")
    print("2. Average Price by Brand")
    print("3. Average Price Grouped by Type")
    print("4. Price vs CPU Tier")
    print("5. Price Distribution: Laptop vs Desktop")
    
    choice = input("\nEnter chart number (1-5): ").strip()
    return choice

# Display single chart
def show_single_chart(chart_num):
    if not os.path.exists(DB_NAME):
        print(f"Error: Database '{DB_NAME}' not found. Please import data first.")
        return
    
    try:
        conn = db.getconn(DB_NAME)
        
        print("Loading data from database...")
        df = charts.get_products_dataframe(conn)
        print(f"Loaded {len(df)} products.\n")
        
        if chart_num == '1':
            charts.show_price_histogram(df)
        elif chart_num == '2':
            charts.show_avg_price_by_brand(df)
        elif chart_num == '3':
            charts.show_avg_price_grouped(df)
        elif chart_num == '4':
            charts.show_price_vs_cpu_tier(df)
        elif chart_num == '5':
            charts.show_box_price_by_type(df)
        else:
            print("Invalid chart number.")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

# Main function loop
def main():
    while True:
        choice = show_menu()
        
        if choice == '1':
            import_data()
        elif choice == '2':
            show_dashboard()
        elif choice == '3':
            show_visualizations()
        elif choice == '4':
            chart_choice = show_chart_menu()
            show_single_chart(chart_choice)
        elif choice == '5':
            print("\nExiting... Goodbye!")
            sys.exit(0)
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
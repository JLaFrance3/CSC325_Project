import os
import sys
import db_functions as db
import chart_functions as charts

# Configuration
DB_NAME = "computers.db"
CSV_PATH = "computer_prices_all.csv" 

def import_data():
    """Import CSV data into the database."""
    # Check if CSV exists before starting
    if not os.path.exists(CSV_PATH):
        print(f"Error: The file '{CSV_PATH}' was not found.")
        return False

    print(f"Connecting to local SQLite database: {DB_NAME}...")

    # 1. Connect (This creates the file if it doesn't exist)
    try:
        conn = db.getconn(DB_NAME)
        cur = conn.cursor()
    except Exception as e:
        print(f"Connection failed: {e}")
        return False

    try:
        # 2. Reset Tables
        db.setup_db(cur)
        conn.commit()

        # 3. Read Data
        # Set to None to read all rows. 
        # For testing, limit to 100 rows.
        data_limit = None
        
        brands, cpus, gpus, products = db.read_data(CSV_PATH, data_limit)

        # 4. Insert Data
        db.insert_brands(cur, brands)
        conn.commit() # Save brands so we can query IDs

        brand_map = db.get_brand_map(cur)

        db.insert_cpus(cur, cpus, brand_map)
        db.insert_gpus(cur, gpus, brand_map)
        
        db.insert_products(cur, products, brand_map)
        conn.commit()
        
        # 5. Show statistics
        stats = db.get_database_stats(cur)
        print("\n" + "="*50)
        print("SUCCESS: All data imported into 'computers.db'")
        print("="*50)
        print(f"  Brands:   {stats['brands']}")
        print(f"  CPUs:     {stats['cpus']}")
        print(f"  GPUs:     {stats['gpus']}")
        print(f"  Products: {stats['products']}")
        print("="*50 + "\n")
        
        return True

    except Exception as e:
        print(f"\nCRITICAL ERROR: {e}")
        conn.rollback()
        return False
    
    finally:
        cur.close()
        conn.close()

def show_visualizations():
    """Display data visualizations from the database."""
    if not os.path.exists(DB_NAME):
        print(f"Error: Database '{DB_NAME}' not found. Please import data first.")
        return

    print(f"Connecting to database: {DB_NAME}...")
    
    try:
        conn = db.getconn(DB_NAME)
        
        print("\nGenerating visualizations...\n")
        
        # Generate all available charts
        print("1. Price Distribution Histogram")
        charts.show_price_histogram(conn)
        
        print("2. Average Price by Brand")
        charts.show_avg_price_by_brand(conn)
        
        print("3. Average Price Grouped by Type")
        charts.show_avg_price_grouped(conn)
        
        print("4. Price vs CPU Tier")
        charts.show_price_vs_cpu_tier(conn)
        
        print("5. Price Distribution: Laptop vs Desktop")
        charts.show_box_price_by_type(conn)
        
        print("\nAll visualizations displayed!")
        
    except Exception as e:
        print(f"Error generating visualizations: {e}")
    
    finally:
        conn.close()

def show_menu():
    """Display interactive menu for user."""
    print("\n" + "="*50)
    print("Computer Price Analysis System")
    print("="*50)
    print("1. Import data from CSV")
    print("2. Show all visualizations")
    print("3. Show specific chart")
    print("4. Exit")
    print("="*50)
    
    choice = input("\nEnter your choice (1-4): ").strip()
    return choice

def show_chart_menu():
    """Display chart selection menu."""
    print("\nAvailable Charts:")
    print("1. Price Distribution Histogram")
    print("2. Average Price by Brand")
    print("3. Average Price Grouped by Type")
    print("4. Price vs CPU Tier")
    print("5. Price Distribution: Laptop vs Desktop")
    
    choice = input("\nEnter chart number (1-5): ").strip()
    return choice

def show_single_chart(chart_num):
    """Show a specific chart."""
    if not os.path.exists(DB_NAME):
        print(f"Error: Database '{DB_NAME}' not found. Please import data first.")
        return
    
    try:
        conn = db.getconn(DB_NAME)
        
        if chart_num == '1':
            charts.show_price_histogram(conn)
        elif chart_num == '2':
            charts.show_avg_price_by_brand(conn)
        elif chart_num == '3':
            charts.show_avg_price_grouped(conn)
        elif chart_num == '4':
            charts.show_price_vs_cpu_tier(conn)
        elif chart_num == '5':
            charts.show_box_price_by_type(conn)
        else:
            print("Invalid chart number.")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

def main():
    """Main program loop."""
    while True:
        choice = show_menu()
        
        if choice == '1':
            import_data()
        elif choice == '2':
            show_visualizations()
        elif choice == '3':
            chart_choice = show_chart_menu()
            show_single_chart(chart_choice)
        elif choice == '4':
            print("\nExiting... Goodbye!")
            sys.exit(0)
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
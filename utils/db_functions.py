"""
Functions used when connecting to supabase database, creating tables, and inserting data
"""

import csv
import psycopg2

# Get connection with supabase db
def getconn():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL not found in .env")
        
    return psycopg2.connect(db_url)
    
# Used for initial setup of db. Reset tables and upload from csv file
def setup_db(cur):
    # Drop exisiting tables
    print("Resetting tables...")
    cur.execute('DROP TABLE IF EXISTS Products CASCADE;')
    cur.execute('DROP TABLE IF EXISTS CPU CASCADE;')
    cur.execute('DROP TABLE IF EXISTS GPU CASCADE;')
    cur.execute('DROP TABLE IF EXISTS Brands CASCADE;')

    # Create Brands table (Postgres uses SERIAL rather than AUTO_INCREMENT)
    cur.execute('''
        CREATE TABLE Brands (
            brandId     SERIAL PRIMARY KEY,
            brand_name  VARCHAR(100) NOT NULL UNIQUE
        );
    ''')
    
    # Create CPU table
    cur.execute('''
        CREATE TABLE CPU (
            cpu_model   VARCHAR(100) NOT NULL PRIMARY KEY,
            brandId     INT NOT NULL,
            cpu_tier    INT,
            cpu_cores   INT,
            cpu_threads INT,
            cpu_base_ghz    DECIMAL(3,1),
            cpu_boost_ghz   DECIMAL(3,1),
            FOREIGN KEY(brandId) REFERENCES Brands(brandId)
        );
    ''')

    # Create GPU table
    cur.execute('''
        CREATE TABLE GPU (
            gpu_model   VARCHAR(100) NOT NULL PRIMARY KEY,
            brandId     INT NOT NULL,
            gpu_tier    INT,
            vram_gb     INT,
            FOREIGN KEY(brandId) REFERENCES Brands(brandId)
        );
    ''')

    # Create Products table
    cur.execute('''
        CREATE TABLE Products (
            productId       SERIAL PRIMARY KEY,
            brandId         INT NOT NULL,
            cpu_model       VARCHAR(100) NOT NULL,
            gpu_model       VARCHAR(100) NOT NULL,
            device_type     VARCHAR(50),
            model           VARCHAR(100) NOT NULL,
            release_year    INT,
            os              VARCHAR(50),
            form_factor     VARCHAR(50),
            ram_gb          INT,
            storage_type    VARCHAR(50),
            storage_gb      INT,
            storage_drive_count INT,
            display_type    VARCHAR(50),
            display_size_in DECIMAL(4,1),
            resolution      VARCHAR(50),
            refresh_hz      INT,
            battery_wh      INT,
            charger_watts   INT,
            psu_watts       INT,
            wifi            VARCHAR(50),
            bluetooth       VARCHAR(50),
            weight_kg       DECIMAL(4,2),
            warranty_months INT,
            price           DECIMAL(10,2) NOT NULL,
            FOREIGN KEY(brandId) REFERENCES Brands(brandId),
            FOREIGN KEY(cpu_model) REFERENCES CPU(cpu_model),
            FOREIGN KEY(gpu_model) REFERENCES GPU(gpu_model)
        );
    ''')

# Read csv file with the computers data
def read_csv(csv_filepath):
    try:
        with open(csv_filepath, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            row_count = 0
            for row in reader:
                if DATA_LIMIT is not None and row_count >= DATA_LIMIT:
                    break
                yield row
                row_count += 1
    except Exception as e:
        print(f"Error reading {csv_filepath}: {e}")

# Read in all data once
def read_data(csv_filepath):
    print("Reading in CSV file data...")
    brands_set = set()
    cpus_set = set()
    gpus_set = set()
    product_rows = []
    
    for row in read_csv(csv_filepath):
        # Add brands if they exist
        if row['brand']: brands_set.add(row['brand'])
        if row['cpu_brand']: brands_set.add(row['cpu_brand'])
        if row['gpu_brand']: brands_set.add(row['gpu_brand'])
        
        # Add CPU tuple
        cpus_set.add((
            row['cpu_model'], row['cpu_brand'], row['cpu_tier'],
            row['cpu_cores'], row['cpu_threads'], row['cpu_base_ghz'], row['cpu_boost_ghz']
        ))
        
        # Add GPU tuple
        gpus_set.add((
            row['gpu_model'], row['gpu_brand'], row['gpu_tier'], row['vram_gb']
        ))
        
        product_rows.append(row)
    
    print("CSV read complete.\n")
    return brands_set, cpus_set, gpus_set, product_rows

# Insert brands into table
def insert_brands(cur, brands_set):
    print("Populating Brands table...")
    cur.execute('USE computer_db')
    count = 0

    for brand_name in brands_set:
        if brand_name:
            try:
                cur.execute('''
                    INSERT INTO Brands (brand_name) 
                    VALUES (%s)
                    ON CONFLICT (brand_name) DO NOTHING
                ''', (brand_name, ))
                count += cur.rowcount
            except Exception as e:
                print(f"Error inserting brand {brand_name}: {e}")
    
    print(f"Brands table populated with {count} new entries.")

# Insert cpus into table
def insert_cpus(cur, cpus_set):
    print("Populating CPU table...")
    count = 0
    skipped = 0

    for entry in cpus_set:
        cpu_model, brand_name, cpu_tier, cpu_cores, cpu_threads, cpu_base_ghz, cpu_boost_ghz = entry
        
        # Convert types
        try:
            cpu_tier = int(cpu_tier) if cpu_tier else None
            cpu_cores = int(cpu_cores) if cpu_cores else None
            cpu_threads = int(cpu_threads) if cpu_threads else None
            cpu_base_ghz = float(cpu_base_ghz) if cpu_base_ghz else None
            cpu_boost_ghz = float(cpu_boost_ghz) if cpu_boost_ghz else None
        except (ValueError, TypeError):
            print(f"Error: Invalid data types for CPU '{cpu_model}'. Skipping.")
            skipped += 1
            continue
        
        # Find Brand ID
        cur.execute('SELECT brandId FROM Brands WHERE brand_name = %s', (brand_name, ))
        result = cur.fetchone()
        
        if result is None:
            print(f"Error: Brand '{brand_name}' not found. Skipping CPU '{cpu_model}'.")
            skipped += 1
            continue
        
        brand_id = result[0]
        
        # Insert entire cpu row
        try:
            cur.execute('''
                INSERT INTO CPU (cpu_model, brandId, cpu_tier, cpu_cores, cpu_threads, cpu_base_ghz, cpu_boost_ghz) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (cpu_model) DO NOTHING
            ''', (cpu_model, brand_id, cpu_tier, cpu_cores, cpu_threads, cpu_base_ghz, cpu_boost_ghz))
            count += cur.rowcount
        except Exception as e:
            print(f"Error inserting CPU {cpu_model}: {e}")
            skipped += 1
    
    print(f"CPU table populated with {count} new entries. ({skipped} skipped)")

# Insert gpus into table
def insert_gpus(cur, gpus_set):
    print("Populating GPU table...")
    count = 0
    skipped = 0

    for entry in gpus_set:
        gpu_model, brand_name, gpu_tier, vram_gb = entry
        
        # Convert types
        try:
            gpu_tier = int(gpu_tier) if gpu_tier else None
            vram_gb = int(vram_gb) if vram_gb else None
        except (ValueError, TypeError):
            print(f"Error: Invalid data types for GPU '{gpu_model}'. Skipping.")
            skipped += 1
            continue
        
        # Find Brand ID
        cur.execute('SELECT brandId FROM Brands WHERE brand_name = %s', (brand_name, ))
        result = cur.fetchone()
        
        if result is None:
            print(f"Error: Brand '{brand_name}' not found. Skipping GPU '{gpu_model}'.")
            skipped += 1
            continue
        
        brand_id = result[0]
        
        # Insert entire gpu row
        try:
            cur.execute('''
                INSERT INTO GPU (gpu_model, brandId, gpu_tier, vram_gb) 
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (gpu_model) DO NOTHING
            ''', (gpu_model, brand_id, gpu_tier, vram_gb))
            count += cur.rowcount
        except Exception as e:
            print(f"Error inserting GPU {gpu_model}: {e}")
            skipped += 1
    
    print(f"GPU table populated with {count} new entries. ({skipped} skipped)")

# Insert products into table
def insert_products(cur, product_rows):
    print("Populating Products table...")
    count = 0
    skipped = 0
    
    for row in product_rows:
        try:
            # Convert types
            price = float(row['price']) if row['price'] else 0.0
            release_year = int(row['release_year']) if row['release_year'] else None
            ram_gb = int(row['ram_gb']) if row['ram_gb'] else None
            storage_gb = int(row['storage_gb']) if row['storage_gb'] else None
            storage_drive_count = int(row['storage_drive_count']) if row['storage_drive_count'] else None
            display_size_in = float(row['display_size_in']) if row['display_size_in'] else None
            refresh_hz = int(row['refresh_hz']) if row['refresh_hz'] else None
            battery_wh = int(row['battery_wh']) if row['battery_wh'] else None
            charger_watts = int(row['charger_watts']) if row['charger_watts'] else None
            psu_watts = int(row['psu_watts']) if row['psu_watts'] else None
            weight_kg = float(row['weight_kg']) if row['weight_kg'] else None
            warranty_months = int(row['warranty_months']) if row['warranty_months'] else None

            # Get Brand ID
            cur.execute('SELECT brandId FROM Brands WHERE brand_name = %s', (row['brand'], ))
            result = cur.fetchone()

            if result is None:
                skipped += 1
                continue
        
            brand_id = result[0]
            
            # Insert entire product row
            cur.execute('''
                INSERT INTO Products 
                (brandId, cpu_model, gpu_model, device_type, model, 
                release_year, os, form_factor, ram_gb, storage_type, 
                storage_gb, storage_drive_count, display_type, display_size_in, 
                resolution, refresh_hz, battery_wh, charger_watts, psu_watts, 
                wifi, bluetooth, weight_kg, warranty_months, price) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s)
            ''', (brand_id, row['cpu_model'], row['gpu_model'], row['device_type'], row['model'], 
                release_year, row['os'], row['form_factor'], ram_gb, row['storage_type'], 
                storage_gb, storage_drive_count, row['display_type'], display_size_in, 
                row['resolution'], refresh_hz, battery_wh, charger_watts, psu_watts, 
                row['wifi'], row['bluetooth'], weight_kg, warranty_months, price))
            
            count += cur.rowcount

        except Exception as e:
            # print(f"Skipping row due to error: {e}") # Uncomment to see specific errors
            skipped += 1
    
    print(f"Products table populated with {count} new entries. ({skipped} skipped)")
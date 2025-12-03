import sqlite3
import csv

def getconn(db_name="computers.db"):
    """Creates a connection to a local SQLite database file."""
    conn = sqlite3.connect(db_name)
    # Enable foreign key constraints (SQLite has them off by default)
    conn.execute("PRAGMA foreign_keys = ON;") 
    return conn

def setup_db(cur):
    print("Resetting tables...")
    # SQLite requires dropping child tables first since it lacks CASCADE DROP
    cur.execute('DROP TABLE IF EXISTS Products;')
    cur.execute('DROP TABLE IF EXISTS CPU;')
    cur.execute('DROP TABLE IF EXISTS GPU;')
    cur.execute('DROP TABLE IF EXISTS Brands;')

    cur.execute('''
        CREATE TABLE Brands (
            brandId     INTEGER PRIMARY KEY AUTOINCREMENT,
            brand_name  TEXT NOT NULL UNIQUE
        );
    ''')
    
    cur.execute('''
        CREATE TABLE CPU (
            cpu_model   TEXT NOT NULL PRIMARY KEY,
            brandId     INTEGER NOT NULL,
            cpu_tier    INTEGER,
            cpu_cores   INTEGER,
            cpu_threads INTEGER,
            cpu_base_ghz    REAL,
            cpu_boost_ghz   REAL,
            FOREIGN KEY(brandId) REFERENCES Brands(brandId)
        );
    ''')

    cur.execute('''
        CREATE TABLE GPU (
            gpu_model   TEXT NOT NULL PRIMARY KEY,
            brandId     INTEGER NOT NULL,
            gpu_tier    INTEGER,
            vram_gb     INTEGER,
            FOREIGN KEY(brandId) REFERENCES Brands(brandId)
        );
    ''')

    cur.execute('''
        CREATE TABLE Products (
            productId       INTEGER PRIMARY KEY AUTOINCREMENT,
            brandId         INTEGER NOT NULL,
            cpu_model       TEXT NOT NULL,
            gpu_model       TEXT NOT NULL,
            device_type     TEXT,
            model           TEXT NOT NULL,
            release_year    INTEGER,
            os              TEXT,
            form_factor     TEXT,
            ram_gb          INTEGER,
            storage_type    TEXT,
            storage_gb      INTEGER,
            storage_drive_count INTEGER,
            display_type    TEXT,
            display_size_in REAL,
            resolution      TEXT,
            refresh_hz      INTEGER,
            battery_wh      INTEGER,
            charger_watts   INTEGER,
            psu_watts       INTEGER,
            wifi            TEXT,
            bluetooth       TEXT,
            weight_kg       REAL,
            warranty_months INTEGER,
            price           REAL NOT NULL,
            FOREIGN KEY(brandId) REFERENCES Brands(brandId),
            FOREIGN KEY(cpu_model) REFERENCES CPU(cpu_model),
            FOREIGN KEY(gpu_model) REFERENCES GPU(gpu_model)
        );
    ''')

def read_csv(csv_filepath, data_limit):
    try:
        with open(csv_filepath, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            row_count = 0
            for row in reader:
                if data_limit is not None and row_count >= data_limit:
                    break
                yield row
                row_count += 1
    except Exception as e:
        print(f"Error reading {csv_filepath}: {e}")

def read_data(csv_filepath, data_limit):
    print("Reading in CSV file data...")
    brands_set = set()
    cpus_set = set()
    gpus_set = set()
    product_rows = []
    
    for row in read_csv(csv_filepath, data_limit):
        if row['brand']: brands_set.add(row['brand'])
        if row['cpu_brand']: brands_set.add(row['cpu_brand'])
        if row['gpu_brand']: brands_set.add(row['gpu_brand'])
        
        cpus_set.add((
            row['cpu_model'], row['cpu_brand'], row['cpu_tier'],
            row['cpu_cores'], row['cpu_threads'], row['cpu_base_ghz'], row['cpu_boost_ghz']
        ))
        
        gpus_set.add((
            row['gpu_model'], row['gpu_brand'], row['gpu_tier'], row['vram_gb']
        ))
        
        product_rows.append(row)
    
    print("CSV read complete.\n")
    return brands_set, cpus_set, gpus_set, product_rows

# --- OPTIMIZED INSERT FUNCTIONS (SQLite Version) ---

def get_brand_map(cur):
    cur.execute("SELECT brand_name, brandId FROM Brands")
    return {row[0]: row[1] for row in cur.fetchall()}

def insert_brands(cur, brands_set):
    print(f"Populating Brands table ({len(brands_set)} items)...")
    if not brands_set: return

    values = [(b,) for b in brands_set if b]
    # SQLite uses ? placeholders and 'executemany'
    cur.executemany("INSERT OR IGNORE INTO Brands (brand_name) VALUES (?)", values)

def insert_cpus(cur, cpus_set, brand_map):
    print(f"Populating CPU table ({len(cpus_set)} items)...")
    values = []
    
    for entry in cpus_set:
        cpu_model, brand_name, cpu_tier, cores, threads, base, boost = entry
        if brand_name not in brand_map: continue

        values.append((
            cpu_model, 
            brand_map[brand_name],
            int(cpu_tier) if cpu_tier else None,
            int(cores) if cores else None,
            int(threads) if threads else None,
            float(base) if base else None,
            float(boost) if boost else None
        ))

    if values:
        query = """
            INSERT OR IGNORE INTO CPU (cpu_model, brandId, cpu_tier, cpu_cores, cpu_threads, cpu_base_ghz, cpu_boost_ghz) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        cur.executemany(query, values)

def insert_gpus(cur, gpus_set, brand_map):
    print(f"Populating GPU table ({len(gpus_set)} items)...")
    values = []

    for entry in gpus_set:
        gpu_model, brand_name, gpu_tier, vram = entry
        if brand_name not in brand_map: continue

        values.append((
            gpu_model, 
            brand_map[brand_name], 
            int(gpu_tier) if gpu_tier else None,
            int(vram) if vram else None
        ))

    if values:
        query = "INSERT OR IGNORE INTO GPU (gpu_model, brandId, gpu_tier, vram_gb) VALUES (?, ?, ?, ?)"
        cur.executemany(query, values)

def insert_products(cur, product_rows, brand_map):
    print(f"Populating Products table ({len(product_rows)} items)...")
    values = []
    
    for row in product_rows:
        if row['brand'] not in brand_map: continue
        
        values.append((
            brand_map[row['brand']],
            row['cpu_model'], row['gpu_model'], row['device_type'], row['model'], 
            int(row['release_year']) if row['release_year'] else None,
            row['os'], row['form_factor'], 
            int(row['ram_gb']) if row['ram_gb'] else None,
            row['storage_type'], 
            int(row['storage_gb']) if row['storage_gb'] else None,
            int(row['storage_drive_count']) if row['storage_drive_count'] else None,
            row['display_type'], 
            float(row['display_size_in']) if row['display_size_in'] else None,
            row['resolution'], 
            int(row['refresh_hz']) if row['refresh_hz'] else None,
            int(row['battery_wh']) if row['battery_wh'] else None,
            int(row['charger_watts']) if row['charger_watts'] else None,
            int(row['psu_watts']) if row['psu_watts'] else None,
            row['wifi'], row['bluetooth'], 
            float(row['weight_kg']) if row['weight_kg'] else None,
            int(row['warranty_months']) if row['warranty_months'] else None,
            float(row['price']) if row['price'] else 0.0
        ))

    if values:
        query = """
            INSERT INTO Products 
            (brandId, cpu_model, gpu_model, device_type, model, release_year, os, form_factor, ram_gb, storage_type, 
            storage_gb, storage_drive_count, display_type, display_size_in, resolution, refresh_hz, battery_wh, 
            charger_watts, psu_watts, wifi, bluetooth, weight_kg, warranty_months, price) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cur.executemany(query, values)
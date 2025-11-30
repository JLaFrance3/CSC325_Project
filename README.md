# CSC325 - Databases Project
Project to demonstrate knowledge obtained during CSC325 - Database Systems. Allows for computer data upload to Supabase and data visualization from queries.
Initially intended for use with GCP but we ran out of free credits. 🤷‍♂️

## Setup
Install Python packages
```
pip install pandas plotly psycopg2-binary python-dotenv
```

Create a .env file in root directory like the .env.example
```
DATABASE_URL=supabase_url
CSV_FILE_PATH=C:/Users/Jim/Documents/computer_prices_all.csv
```

## Usage
#### Default: Upload data from the computer_prices_all.csv
```
python main.py
```

#### Generate charts
```
python main.py --chart all
```
```
python main.py --chart [option]
```
##### Chart options
- price - Average Price by Manufacturer (Bar Chart)
- grouped-price	- Average Price by Brand & Device Type (Grouped Bar Chart)
- boxed-price	- Price Range: Laptop vs Desktop (Box Plot)
- frequency-price	- Distribution of Prices (Histogram)
- tier-price - Price vs CPU Performance Tier (Scatter Plot)

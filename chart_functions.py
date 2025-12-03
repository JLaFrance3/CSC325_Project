"""
Functions used for creating charts and querying data
"""

import pandas as pd
import plotly.express as px
import db_functions as db

def get_products_dataframe(conn):
    """
    Query all products with joined brand and CPU tier information.
    Returns a pandas DataFrame ready for visualization.
    """
    query = """
    SELECT 
        p.*,
        b.brand_name as brand,
        c.cpu_tier,
        g.gpu_tier
    FROM Products p
    JOIN Brands b ON p.brandId = b.brandId
    JOIN CPU c ON p.cpu_model = c.cpu_model
    JOIN GPU g ON p.gpu_model = g.gpu_model
    """
    return pd.read_sql_query(query, conn)

# Option: frequency-price
# Creates histogram of laptop vs desktop prices
def show_price_histogram(conn):
    df = get_products_dataframe(conn)
    fig = px.histogram(
        df,
        x='price',
        nbins=20, 
        title='Distribution of Computer Prices',
        labels={'price': 'Price ($)'}
    )
    fig.show()

# Option: price
# Creates bar chart of average manufacturer prices
def show_avg_price_by_brand(conn):
    df = get_products_dataframe(conn)
    avg_data = df.groupby('brand')['price'].mean().reset_index()
    avg_data = avg_data.sort_values('price', ascending=False)
    fig = px.bar(
        avg_data,
        x='brand',
        y='price', 
        title='Average Price by Manufacturer',
        labels={'price': 'Average Price ($)', 'brand': 'Manufacturer'}
    )
    fig.show()

# Option: grouped-price
# Creates grouped bar chart (laptop, desktop) of average manufacturer prices
def show_avg_price_grouped(conn):
    df = get_products_dataframe(conn)
    avg_data = df.groupby(['brand', 'device_type'])['price'].mean().reset_index()
    fig = px.bar(
        avg_data,
        x='brand',
        y='price',
        color='device_type',
        barmode='group',
        title='Average Price by Manufacturer & Type',
        labels={'price': 'Average Price ($)', 'brand': 'Manufacturer', 'device_type': 'Device Type'}
    )
    fig.show()

# Option: tier-price
# Creates scatter plot of computer prices by tier
def show_price_vs_cpu_tier(conn):
    df = get_products_dataframe(conn)
    df = df.sort_values('cpu_tier')
    fig = px.scatter(
        df,
        x='cpu_tier',
        y='price',
        hover_data=['model', 'brand'],
        title='Price vs. CPU Tier',
        labels={'cpu_tier': 'CPU Tier', 'price': 'Price ($)'},
        color='device_type'
    )
    fig.show()

# Option: boxed-price
# Creates box chart of laptop and desktop pricing
def show_box_price_by_type(conn):
    df = get_products_dataframe(conn)
    fig = px.box(
        df,
        x='device_type',
        y='price', 
        title='Price Distribution: Laptop vs Desktop',
        labels={'price': 'Price ($)', 'device_type': 'Device Type'},
        color='device_type'
    )
    fig.show()
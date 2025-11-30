"""
Functions used for creating charts and querying data
"""

import pandas as pd
import plotly.express as px

# Option: frequency-price
# Creates histogram of laptop vs desktop prices
def show_price_histogram(df):
    fig = px.histogram(
        df,
        x='price',
        nbins=20, 
        title='Distribution of Computer Prices',
        labels={'price': 'Price'}
    )
    fig.show()

# Option: price
# Creates bar chart of avergage manufacturer prices
def show_avg_price_by_brand(df):
    avg_data = df.groupby('brand')['price'].mean().reset_index()
    fig = px.bar(
        avg_data,
        x='brand',
        y='price', 
        title='Average Price by Manufacturer',
        labels={'price': 'Average Price', 'brand': 'Manufacturer'}
    )
    fig.show()

# Option: grouped-price
# Creates grouped bar chart (laptop, desktop) of average manufacturer prices
def show_avg_price_grouped(df):
    avg_data = df.groupby(['brand', 'device_type'])['price'].mean().reset_index()
    fig = px.bar(
        avg_data,
        x='brand',
        y='price',
        color='device_type',
        barmode='group',
        title='Average Price by Manufacturer & Type',
    )
    fig.show()

# Option: tier-price
# Creates scatter plot of computer prices by tier
def show_price_vs_cpu_tier(df):
    df = df.sort_values('cpu_tier')
    fig = px.scatter(
        df,
        x='cpu_tier',
        y='price',
        hover_data=['model', 'brand'],
        title='Price vs. CPU Tier',
        labels={'cpu_tier': 'CPU Tier', 'price': 'Price ($)'},
    )
    fig.show()

# Option: boxed-price
# Creates box chart of laptop and desktop pricing
def show_box_price_by_type(df):
    fig = px.box(
        df,
        x='device_type',
        y='price', 
        title='Price Distribution: Laptop vs Desktop',
        color='device_type'
    )
    fig.show()
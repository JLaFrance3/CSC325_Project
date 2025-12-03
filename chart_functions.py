"""
Functions used for creating charts and querying data
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import db_functions as db

# Query all products with joined brand and CPU tier information. Returns dataframe
def get_products_dataframe(conn):
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

# Price distribution
def show_price_histogram(df):
    price_data = df['price'].dropna()
    counts, bin_edges = np.histogram(price_data, bins=20)
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
    
    hist_df = pd.DataFrame({
        'Price ($)': bin_centers,
        'Count': counts
    })
    
    fig = px.bar(
        hist_df,
        x='Price ($)',
        y='Count', 
        title='Distribution of Computer Prices (Aggregated)',
        labels={'Price ($)': 'Price ($)', 'Count': 'Frequency'}
    )
    
    fig.update_layout(bargap=0.05) 
    fig.show()

# Brand price averages
def show_avg_price_by_brand(df):
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

# Brand price grouped by device type
def show_avg_price_grouped(df):
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

# Scatter of price vs cpu tier
def show_price_vs_cpu_tier(df):
    df = df.sort_values('cpu_tier')
    fig = px.scatter(
        df,
        x='cpu_tier',
        y='price',
        hover_data=['model', 'brand'],
        title='Price vs. CPU Tier (WebGL)',
        labels={'cpu_tier': 'CPU Tier', 'price': 'Price ($)'},
        color='device_type',
        render_mode='webgl'
    )
    fig.show()

# Box/whisker for device type
def show_box_price_by_type(df):
    fig = px.box(
        df,
        x='device_type',
        y='price', 
        title='Price Distribution: Laptop vs Desktop',
        labels={'price': 'Price ($)', 'device_type': 'Device Type'},
        color='device_type'
    )
    fig.show()

# Display all charts on single dashboard
def show_dashboard(df):
    fig = make_subplots(
        rows=3, cols=2,
        subplot_titles=(
            'Distribution of Computer Prices',
            'Average Price by Manufacturer',
            'Average Price by Manufacturer & Type',
            'Price vs. CPU Tier',
            'Price Distribution: Laptop vs Desktop',
            '' 
        ),
        specs=[
            [{'type': 'bar'}, {'type': 'bar'}],
            [{'type': 'bar'}, {'type': 'scatter'}],
            [{'type': 'box'}, {'type': 'table'}]
        ],
        vertical_spacing=0.12,
        horizontal_spacing=0.1
    )
    
    price_data = df['price'].dropna()
    counts, bin_edges = np.histogram(price_data, bins=20)
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
    
    fig.add_trace(
        go.Bar(x=bin_centers, y=counts, name='Price Distribution', showlegend=False),
        row=1, col=1
    )
    
    avg_data = df.groupby('brand')['price'].mean().reset_index()
    avg_data = avg_data.sort_values('price', ascending=False).head(10)  # Top 10
    
    fig.add_trace(
        go.Bar(x=avg_data['brand'], y=avg_data['price'], name='Avg Price', showlegend=False),
        row=1, col=2
    )
    
    grouped_data = df.groupby(['brand', 'device_type'])['price'].mean().reset_index()
    
    for device_type in grouped_data['device_type'].unique():
        subset = grouped_data[grouped_data['device_type'] == device_type]
        subset = subset.sort_values('price', ascending=False).head(10)
        fig.add_trace(
            go.Bar(x=subset['brand'], y=subset['price'], name=device_type),
            row=2, col=1
        )
    
    df_sorted = df.sort_values('cpu_tier')
    
    for device_type in df_sorted['device_type'].unique():
        subset = df_sorted[df_sorted['device_type'] == device_type]
        fig.add_trace(
            go.Scattergl(
                x=subset['cpu_tier'],
                y=subset['price'],
                mode='markers',
                name=device_type,
                marker=dict(size=4, opacity=0.6),
                text=subset['model'],
                hovertemplate='<b>%{text}</b><br>CPU Tier: %{x}<br>Price: $%{y}<extra></extra>'
            ),
            row=2, col=2
        )
    
    for device_type in df['device_type'].unique():
        subset = df[df['device_type'] == device_type]
        fig.add_trace(
            go.Box(y=subset['price'], name=device_type, showlegend=False),
            row=3, col=1
        )
    
    stats = df.groupby('device_type')['price'].agg(['count', 'mean', 'median', 'min', 'max']).reset_index()
    stats.columns = ['Type', 'Count', 'Mean', 'Median', 'Min', 'Max']
    stats['Mean'] = stats['Mean'].round(2)
    stats['Median'] = stats['Median'].round(2)
    
    fig.add_trace(
        go.Table(
            header=dict(
                values=list(stats.columns),
                fill_color='paleturquoise',
                align='left',
                font=dict(size=12, color='black')
            ),
            cells=dict(
                values=[stats[col] for col in stats.columns],
                fill_color='lavender',
                align='left',
                font=dict(size=11)
            )
        ),
        row=3, col=2
    )
    
    fig.update_layout(
        title_text="Computer Price Analysis Dashboard",
        title_font_size=20,
        height=1400,
        showlegend=True,
        legend=dict(
            orientation="v",
            yanchor="top",
            y=0.68,
            xanchor="left",
            x=1.02
        )
    )
    
    fig.update_xaxes(title_text="Price ($)", row=1, col=1)
    fig.update_yaxes(title_text="Frequency", row=1, col=1)
    
    fig.update_xaxes(title_text="Manufacturer", row=1, col=2)
    fig.update_yaxes(title_text="Avg Price ($)", row=1, col=2)
    
    fig.update_xaxes(title_text="Manufacturer", row=2, col=1)
    fig.update_yaxes(title_text="Avg Price ($)", row=2, col=1)
    
    fig.update_xaxes(title_text="CPU Tier", row=2, col=2)
    fig.update_yaxes(title_text="Price ($)", row=2, col=2)
    
    fig.update_xaxes(title_text="Device Type", row=3, col=1)
    fig.update_yaxes(title_text="Price ($)", row=3, col=1)
    
    fig.show()
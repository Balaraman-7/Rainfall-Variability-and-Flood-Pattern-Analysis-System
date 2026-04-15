import io
import urllib, base64
import pandas as pd
import numpy as np
from pymongo import MongoClient
import matplotlib
import matplotlib.pyplot as plt

# Use Agg backend for matplotlib to avoid GUI crashes
matplotlib.use('Agg')

MONGO_URI = 'mongodb://localhost:27017/'
DB_NAME = 'rainfall_analyzer'
COLLECTION_NAME = 'rainfall_data'

def get_collection():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME][COLLECTION_NAME]

def fetch_data(filters=None):
    """
    Fetch data from MongoDB based on a dictionary of filters and return a Pandas DataFrame.
    """
    if filters is None:
        filters = {}
        
    collection = get_collection()
    
    # Build strict mongo query mapping
    query = {}
    if 'region' in filters and filters['region']:
        query['region'] = filters['region']
    if 'year' in filters and filters['year']:
        try:
            query['year'] = int(filters['year'])
        except ValueError:
            pass

    # Retrieve
    cursor = collection.find(query, {"_id": 0})
    data = list(cursor)
    
    if not data:
        return pd.DataFrame()
        
    return pd.DataFrame(data)

def get_distinct_regions():
    return get_collection().distinct("region")
    
def get_distinct_years():
    return get_collection().distinct("year")

def generate_base64_chart(fig):
    """
    Convert a matplotlib figure to a base64 encoded string.
    """
    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches="tight", transparent=True)
    buf.seek(0)
    string = base64.b64encode(buf.read())
    uri = urllib.parse.quote(string)
    plt.close(fig) # Prevent memory leaks
    return uri

def generate_trend_chart(df, x_col='year', y_col='annual', title="Rainfall Trend over Years"):
    """
    Generate a Line Chart for rainfall trend over the years.
    """
    if df.empty:
        return None
        
    # Aggregate if multiple regions
    df_agg = df.groupby(x_col)[y_col].mean().reset_index()

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(df_agg[x_col], df_agg[y_col], marker='o', color='#3b82f6', linewidth=2)
    ax.set_title(title, color='#1e3a8a')
    ax.set_xlabel(x_col.capitalize())
    ax.set_ylabel(f'Mean {y_col.capitalize()} Rainfall (mm)')
    ax.grid(True, linestyle='--', alpha=0.6)
    
    # Modern aesthetic touches
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    return generate_base64_chart(fig)

def generate_bar_comparison_chart(df, group_col='region', val_col='annual', title="Region Comparison"):
    """
    Generate Bar Chart comparing total rainfall across regions/states.
    """
    if df.empty:
        return None
        
    df_agg = df.groupby(group_col)[val_col].mean().reset_index()
    df_agg = df_agg.sort_values(by=val_col, ascending=False).head(15) # Top 15 to avoid clutter

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(df_agg[group_col], df_agg[val_col], color='#10b981')
    ax.set_title(title, color='#065f46')
    ax.set_xlabel(group_col.capitalize())
    ax.set_ylabel(f'Mean {val_col.capitalize()} Rainfall (mm)')
    
    plt.xticks(rotation=45, ha="right")
    ax.grid(True, axis='y', linestyle='--', alpha=0.6)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    return generate_base64_chart(fig)

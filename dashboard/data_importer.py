import os
import pandas as pd
from pymongo import MongoClient

# MongoDB Connection
MONGO_URI = 'mongodb://localhost:27017/'
DB_NAME = 'rainfall_analyzer'
COLLECTION_NAME = 'rainfall_data'

def get_db_collection():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[COLLECTION_NAME]

def clean_and_import_data(csv_path, chunk_size=5000):
    """
    Reads CSV in chunks, cleans data using Pandas, and inserts into MongoDB via PyMongo.
    """
    collection = get_db_collection()
    
    # Initialize count
    initial_count = collection.count_documents({})
    if initial_count > 0:
        print(f"Collection already has {initial_count} records. Proceeding to drop and re-import for a fresh build.")
        collection.drop()

    print(f"Starting import from {csv_path}...")
    total_inserted = 0

    try:
        # Read in chunks to prevent memory overload with large datasets
        for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
            # Clean data:
            # 1. Normalize column names (lowercase, replace spaces/hyphens with underscores)
            chunk.columns = chunk.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')
            
            # Rename some cols for clearer semantics
            chunk.rename(columns={'subdivision': 'region'}, inplace=True)
            
            # 2. Fill missing values or drop them depending on requirement
            # For rainfall, numeric columns missing can be filled with 0.0 or dropped.
            numeric_cols = chunk.select_dtypes(include=['float64', 'int64']).columns
            chunk[numeric_cols] = chunk[numeric_cols].fillna(0.0)

            # Convert to dictionary format (list of dicts)
            records = chunk.to_dict('records')
            
            # 3. Bulk Insert into MongoDB
            if records:
                collection.insert_many(records)
                total_inserted += len(records)
                print(f"Inserted {total_inserted} records so far...")

        print(f"Data Import Complete! Total records: {total_inserted}")

        # Create indexing to optimize queries later (by state/region, year)
        print("Creating MongoDB Indexes...")
        collection.create_index([("region", 1)])
        collection.create_index([("year", 1)])
        collection.create_index([("region", 1), ("year", 1)])
        print("Indexes created.")

    except Exception as e:
        print(f"Error during import: {e}")

if __name__ == "__main__":
    csv_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'rainfall in india 1901-2015.csv')
    if os.path.exists(csv_file):
        clean_and_import_data(csv_file)
    else:
        print(f"CSV file not found at: {csv_file}")

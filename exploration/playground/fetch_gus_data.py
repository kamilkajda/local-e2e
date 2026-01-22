import requests
import pandas as pd
import json

def run_playground():
    # Setup API endpoint
    # Variable: 64428 (Average monthly gross wages)
    variable_id = 64428
    url = f"https://bdl.stat.gov.pl/api/v1/data/by-variable/{variable_id}"
    
    # 2 = Voivodships (Województwa)
    params = {
        'unit-level': 2,
        'page-size': 100,
        'format': 'json'
    }

    # Fetch data
    print(f"--- Fetching Data for Variable {variable_id} ---")
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    data = response.json()
    raw_results = data.get('results', [])
    print(f"Fetched {len(raw_results)} units")

    # --- JSON NORMALIZE APPROACH ---
    # Flatten nested structure: Unit -> values[] -> {year, val}
    df = pd.json_normalize(
        data=raw_results, 
        record_path='values', 
        meta=['id', 'name']
    )

    # Rename columns to match desired schema
    df = df.rename(columns={'val': 'value', 'id': 'unit_id', 'name': 'unit_name'})

    # Add Variable ID column (Identifies WHAT this data represents in a merged table)
    df['variable_id'] = variable_id

    # Select only relevant columns (Dropped 'attrId')
    # Keeping 'unit_id' as a foreign key for future Dimension Tables
    final_columns = ['variable_id', 'unit_id', 'unit_name', 'year', 'value']
    df = df[final_columns]

    # Display results
    print("\n--- DataFrame Output (Cleaned) ---")
    print(df.head(10))
    print(f"\nDataFrame Shape: {df.shape}")
    
    # Verify data types
    print("\n--- Data Types ---")
    print(df.dtypes)

if __name__ == "__main__":
    run_playground()
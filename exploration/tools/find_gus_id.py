import requests
import json
import os
import sys

def search_variables(query):
    """
    Searches for variables in GUS API matching the FULL phrase.
    It fetches multiple pages to ensure we don't miss anything.
    Displays results in a hierarchical format (Category > Variable).
    """
    url = "https://bdl.stat.gov.pl/api/v1/variables/search"
    
    print(f"--- GUS API SEARCH ---")
    print(f"Query phrase: '{query}'")
    print(f"Fetching pages (limit: 20 pages / 2000 items)...")
    
    params = {
        'name': query,
        'page-size': 100,
        'lang': 'pl',
        'format': 'json'
    }
    
    all_results = []
    page = 0
    max_pages = 20
    
    try:
        while page < max_pages:
            params['page'] = page
            print(".", end="", flush=True)
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            results = data.get('results', [])
            if not results:
                break
                
            all_results.extend(results)
            
            if len(results) < params['page-size']:
                break
            
            page += 1
        
        print(f"\nDone. Found {len(all_results)} records.")
        
        if not all_results:
            print("No variables found. Try a shorter or more general phrase.")
            return

        print("-" * 120)
        print(f"{'ID':<8} | {'Unit':<10} | {'Hierarchy context > [VARIABLE NAME]'}")
        print("-" * 120)
        
        for var in all_results:
            var_id = var['id']
            unit = var.get('measureUnitName', '-')
            
            # Construct hierarchy path from n5 down to n2
            hierarchy_parts = [var.get(key) for key in ['n5', 'n4', 'n3', 'n2'] if var.get(key)]
            path = " > ".join(hierarchy_parts)
            
            variable_name = var.get('n1', '???')
            
            if path:
                full_display = f"{path} > [{variable_name}]"
            else:
                full_display = f"[{variable_name}]"
            
            print(f"{var_id:<8} | {unit:<10} | {full_display}")

    except Exception as e:
        print(f"\nError: {e}")

def check_variable_by_id(var_id):
    """
    Fetches details for a specific variable ID directly from API.
    Useful for verifying metadata (units, full name).
    """
    url = f"https://bdl.stat.gov.pl/api/v1/variables/{var_id}"
    params = {'lang': 'pl', 'format': 'json'}
    
    print(f"Inspecting ID: {var_id}...")
    
    try:
        response = requests.get(url, params=params)
        
        if response.status_code == 404:
            print(f"   [!] ID {var_id} not found in GUS API.")
            return
            
        response.raise_for_status()
        data = response.json()
        
        n1 = data.get('n1', '')
        level_name = " > ".join([data.get(k) for k in ['n5', 'n4', 'n3', 'n2'] if data.get(k)])
        if level_name:
            full_name = f"{level_name} > [{n1}]"
        else:
            full_name = f"[{n1}]"
            
        unit = data.get('measureUnitName', '-')
        subject_id = data.get('subjectId', '-')
        
        print(f"   Name: {full_name}")
        print(f"   Unit: {unit}")
        print(f"   Subject ID: {subject_id}")
        print("-" * 60)

    except Exception as e:
        print(f"Error fetching ID {var_id}: {e}")

if __name__ == "__main__":
    # --- AUTO-DETECT DEV CONFIGURATION PATTERN ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(script_dir))) 
    
    # MODE SWITCH:
    
    # 1. Search for a phrase
    # search_phrase = "nakłady inwestycyjne na 1 mieszkańca"
    # search_variables(search_phrase)

    # 2. Inspect specific IDs (Verification of new metrics)
    ids_to_check = [72305, 748601, 633101, 633617]
    for i in ids_to_check:
        check_variable_by_id(i)
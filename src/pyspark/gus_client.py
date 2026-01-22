import requests
import json
import time
import os

class GusClient:
    """
    Client for fetching data from Statistics Poland (GUS) BDL API.
    Designed for the Extract phase of ELT - dumps raw JSONs to disk.
    Supports authenticated requests via API Key.
    """
    
    def __init__(self, api_key=None):
        self.base_url = "https://bdl.stat.gov.pl/api/v1"
        self.headers = {} 
        
        # If API Key is provided, add it to headers to increase limits
        if api_key:
            self.headers['X-ClientId'] = api_key
            print("   [GusClient] API Key loaded. High limits enabled.")
        else:
            print("   [GusClient] No API Key provided. Running in anonymous mode (lower limits).")

    def download_variable(self, variable_id, variable_name, output_base_dir, unit_level=2, force=False):
        """
        Downloads all pages for a variable and saves them as JSON files.
        Skips download if data exists, unless force=True.
        
        :param variable_id: GUS variable ID
        :param variable_name: Human-readable name (used for folder naming)
        :param output_base_dir: Base path for raw data
        :param unit_level: 2 for Voivodships
        :param force: If True, redownloads data even if files exist
        """
        target_dir = os.path.join(output_base_dir, variable_name)
        os.makedirs(target_dir, exist_ok=True)
        
        # Check if data already exists to avoid API spam
        if not force:
            existing_files = [f for f in os.listdir(target_dir) if f.endswith('.json')]
            if existing_files:
                print(f"   [GusClient] Skipping '{variable_name}' (Data exists). Use --force to redownload.")
                return

        print(f"   [GusClient] Downloading '{variable_name}' (ID: {variable_id}) to {target_dir}...")

        endpoint = f"{self.base_url}/data/by-variable/{variable_id}"
        params = {
            'unit-level': unit_level,
            'page-size': 100,
            'format': 'json',
            'lang': 'en'
        }
        
        page = 0
        total_records = 0

        while True:
            params['page'] = page
            try:
                response = requests.get(endpoint, params=params, headers=self.headers)
                response.raise_for_status()
                
                data = response.json()
                results = data.get('results', [])
                
                if not results:
                    break
                
                # Save raw JSON page to disk
                filename = f"{variable_name}_lvl{unit_level}_p{page}.json"
                file_path = os.path.join(target_dir, filename)
                
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=4, ensure_ascii=False)
                
                count = len(results)
                total_records += count

                # Pagination check
                if 'links' in data and 'next' in data['links']:
                    page += 1
                    # With API key we can be faster, but keeping small delay is good practice
                    time.sleep(0.1) 
                else:
                    break
                    
            except Exception as e:
                print(f"      [ERROR] Failed on page {page}: {e}")
                break
                
        print(f"   [GusClient] Finished. Total records saved: {total_records}")
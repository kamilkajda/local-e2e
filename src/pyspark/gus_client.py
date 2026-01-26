import requests
import json
import time
import os

class GusClient:
    """
    Client for fetching data from Statistics Poland (GUS) BDL API.
    """
    
    def __init__(self, api_key=None):
        self.base_url = "https://bdl.stat.gov.pl/api/v1"
        self.headers = {} 
        if api_key:
            self.headers['X-ClientId'] = api_key

    def download_all_metrics(self, metrics, output_base_dir, levels=[0, 2], force=False):
        """
        Orchestrates the download of all metrics defined in configuration.
        """
        print(f"   [GusClient] Starting bulk download for levels: {levels}")
        for metric in metrics:
            if not metric.get('variable_id'): continue
            self.download_variable(
                variable_id=metric['variable_id'],
                variable_name=metric['name'],
                output_base_dir=output_base_dir,
                unit_levels=levels,
                force=force
            )

    def download_variable(self, variable_id, variable_name, output_base_dir, unit_levels=[0, 2], force=False):
        """
        Downloads data for a specific variable across multiple unit levels.
        Sets source language to 'pl' for stable metadata mapping.
        """
        target_dir = os.path.join(output_base_dir, variable_name)
        os.makedirs(target_dir, exist_ok=True)
        
        for level in unit_levels:
            if not force:
                existing_files = [f for f in os.listdir(target_dir) if f"lvl{level}" in f]
                if existing_files:
                    continue

            print(f"      -> Downloading '{variable_name}' Lvl {level}...")
            endpoint = f"{self.base_url}/data/by-variable/{variable_id}"
            # lang set to 'pl' to ensure we have a stable source for our English translation map
            params = {'unit-level': level, 'page-size': 100, 'format': 'json', 'lang': 'pl'}
            
            page = 0
            while True:
                params['page'] = page
                try:
                    response = requests.get(endpoint, params=params, headers=self.headers)
                    response.raise_for_status()
                    data = response.json()
                    results = data.get('results', [])
                    
                    if not results: break
                    
                    file_path = os.path.join(target_dir, f"{variable_name}_lvl{level}_p{page}.json")
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(data, f, indent=4, ensure_ascii=False)
                    
                    if 'links' in data and 'next' in data['links']:
                        page += 1
                        time.sleep(0.1)
                    else:
                        break
                except Exception as e:
                    print(f"         [ERROR] Lvl {level} P{page}: {e}")
                    break
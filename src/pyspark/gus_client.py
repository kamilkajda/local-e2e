import requests
import json
import time
import os

class GusClient:
    """
    Interface for the Statistics Poland (GUS) BDL REST API. 
    Handles authentication, hierarchical data retrieval, and local persistence.
    """
    
    def __init__(self, api_key=None):
        """
        Initializes the API client.
        :param api_key: Optional X-ClientId for elevated rate limits.
        """
        self.base_url = "https://bdl.stat.gov.pl/api/v1"
        self.headers = {} 
        if api_key:
            self.headers['X-ClientId'] = api_key

    def download_all_metrics(self, metrics, output_base_dir, levels=[0, 2], force=False):
        """
        Executes bulk data extraction based on the provided metrics schema.
        :param metrics: List of metric definitions containing variable_ids.
        :param output_base_dir: Target root for JSON persistence.
        :param levels: Hierarchical unit levels (e.g., 0=National, 2=Voivodeship).
        :param force: If True, bypasses local file existence checks.
        """
        print(f"   [GusClient] Initiating bulk extraction for levels: {levels}")
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
        Retrieves time-series data for a specific variable ID. 
        Implements pagination and ensures metadata stability via fixed language parameters.
        """
        target_dir = os.path.join(output_base_dir, variable_name)
        os.makedirs(target_dir, exist_ok=True)
        
        for level in unit_levels:
            # Idempotency check: Skip download if level-specific files already exist
            if not force:
                existing_files = [f for f in os.listdir(target_dir) if f"lvl{level}" in f]
                if existing_files:
                    continue

            print(f"      -> Extracting: '{variable_name}' (Level {level})")
            endpoint = f"{self.base_url}/data/by-variable/{variable_id}"
            
            # lang='pl' is enforced to maintain consistent source keys for the transformation layer
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
                    
                    # Atomic persistence of paginated JSON results
                    file_path = os.path.join(target_dir, f"{variable_name}_lvl{level}_p{page}.json")
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(data, f, indent=4, ensure_ascii=False)
                    
                    # Pagination flow control
                    if 'links' in data and 'next' in data['links']:
                        page += 1
                        time.sleep(0.1) # Throttling to respect API rate limits
                    else:
                        break
                except Exception as e:
                    print(f"         [ERROR] Extraction failed at Level {level}, Page {page}: {e}")
                    break
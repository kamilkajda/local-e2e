import sys
import os
import json
import requests

# --- ENVIRONMENT SETUP ---
current_dir = os.path.dirname(os.path.abspath(__file__))
# Go up 2 levels: exploration/debug -> exploration -> project_root
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(os.path.join(project_root, 'src', 'pyspark'))

try:
    from utils import load_config
except ImportError as e:
    print(f"Error importing modules: {e}")
    sys.exit(1)

def check_poland_availability():
    print("--- GUS API: POLAND LEVEL (UNIT-LEVEL=0) AVAILABILITY CHECK ---")

    # 1. Load Config (to get API Key if available)
    config_path = os.path.join(project_root, "configs", "dev", "settings.json")
    config = load_config(config_path)
    api_key = config.get('gus', {}).get('api_key')
    
    headers = {}
    if api_key:
        headers['X-ClientId'] = api_key

    # 2. Load Metrics Definition
    metrics_path = os.path.join(project_root, "configs", "gus_metrics.json")
    with open(metrics_path, 'r', encoding='utf-8') as f:
        metrics = json.load(f)

    print(f"{'METRIC NAME':<45} | {'STATUS':<10} | {'MIN YEAR':<8} | {'MAX YEAR':<8}")
    print("-" * 80)

    for metric in metrics:
        name = metric['name']
        var_id = metric['variable_id']
        
        # Endpoint for data by variable
        url = f"https://bdl.stat.gov.pl/api/v1/data/by-variable/{var_id}"
        params = {
            'unit-level': 0, # Level 0 = Poland
            'format': 'json',
            'page-size': 100
        }

        try:
            response = requests.get(url, params=params, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                
                if results:
                    # Level 0 usually returns only one result object for Poland
                    values = results[0].get('values', [])
                    if values:
                        years = [int(v['year']) for v in values if v.get('year')]
                        print(f"{name:<45} | {'OK':<10} | {min(years):<8} | {max(years):<8}")
                    else:
                        print(f"{name:<45} | {'EMPTY':<10} | {'-':<8} | {'-':<8}")
                else:
                    print(f"{name:<45} | {'NO DATA':<10} | {'-':<8} | {'-':<8}")
            else:
                print(f"{name:<45} | {'API ERR':<10} | {'-':<8} | {'-':<8} (Code: {response.status_code})")
                
        except Exception as e:
            print(f"{name:<45} | {'ERROR':<10} | {str(e)[:20]}")

if __name__ == "__main__":
    check_poland_availability()
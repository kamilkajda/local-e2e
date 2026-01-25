import sys
import os
import json
import glob

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

def check_data_ranges():
    print("--- RAW DATA AVAILABILITY CHECK (JSON) ---")

    # 1. Load Config to find data path
    config_path = os.path.join(project_root, "configs", "dev", "settings.json")
    if not os.path.exists(config_path):
        print(f"[!] Config not found at {config_path}")
        return
        
    config = load_config(config_path)
    # Ensure raw_data_dir is absolute
    raw_data_dir = os.path.abspath(os.path.join(project_root, config['paths']['raw']))
    
    # 2. Load Metrics Definition
    metrics_path = os.path.join(project_root, "configs", "gus_metrics.json")
    with open(metrics_path, 'r', encoding='utf-8') as f:
        metrics = json.load(f)

    print(f"\nScanning directory: {raw_data_dir}")
    print(f"{'METRIC NAME':<45} | {'MIN YEAR':<10} | {'MAX YEAR':<10} | {'RECORDS':<10}")
    print("-" * 85)

    # 3. Iterate and Check Local Files
    for metric in metrics:
        name = metric['name']
        metric_dir = os.path.join(raw_data_dir, name)
        
        years = []
        record_count = 0
        
        # Find all JSON files for this metric
        json_files = glob.glob(os.path.join(metric_dir, "*.json"))
        
        if not json_files:
            print(f"{name:<45} | {'NO FILES':<34}")
            continue
            
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    results = data.get('results', [])
                    
                    for unit in results:
                        # GUS Structure: results -> values -> year
                        values = unit.get('values', [])
                        record_count += len(values)
                        for val in values:
                            y = val.get('year')
                            if y:
                                years.append(int(y))
                                
            except Exception as e:
                print(f"[!] Error reading {os.path.basename(json_file)}: {e}")

        if years:
            min_y = min(years)
            max_y = max(years)
            print(f"{name:<45} | {min_y:<10} | {max_y:<10} | {record_count:<10}")
        else:
            print(f"{name:<45} | {'EMPTY':<34}")

if __name__ == "__main__":
    check_data_ranges()
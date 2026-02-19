import glob
import json
import os
import sys
from collections import defaultdict

# --- ENVIRONMENT SETUP ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(os.path.join(project_root, "src", "pyspark"))

try:
    from utils import load_config
except ImportError as e:
    print(f"Error importing modules: {e}")
    sys.exit(1)


def check_detailed_availability():
    """
    Scans raw JSON files and identifies data gaps per region (voivodeship).
    Flags missing years for specific regions compared to the metric's global year set.
    """
    print("--- RAW DATA REGIONAL GAP ANALYSIS ---")

    # 1. Load Config
    config_path = os.path.join(project_root, "configs", "dev", "settings.json")
    if not os.path.exists(config_path):
        print(f"[!] Config not found at {config_path}")
        return

    config = load_config(config_path)
    raw_data_dir = os.path.abspath(os.path.join(project_root, config["paths"]["raw"]))

    # 2. Load Metrics Definition
    metrics_path = os.path.join(project_root, "configs", "gus_metrics.json")
    with open(metrics_path, "r", encoding="utf-8") as f:
        metrics = json.load(f)

    print(f"\nScanning: {raw_data_dir}")
    print("-" * 100)

    # 3. Check Files Metric by Metric
    for metric in metrics:
        name = metric["name"]
        metric_dir = os.path.join(raw_data_dir, name)

        # Structure: { region_name: set(years) }
        region_years = defaultdict(set)
        global_years = set()

        json_files = glob.glob(os.path.join(metric_dir, "*.json"))

        if not json_files:
            continue

        for json_file in json_files:
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    results = data.get("results", [])

                    for unit in results:
                        reg_name = unit.get("name", "Unknown")
                        values = unit.get("values", [])
                        for val in values:
                            y = val.get("year")
                            if y:
                                y_int = int(y)
                                region_years[reg_name].add(y_int)
                                global_years.add(y_int)

            except Exception:
                pass

        if global_years:
            print(f"\n[METRIC] {name.upper()}")
            sorted_global = sorted(list(global_years))
            print(f"Global Year Set: {min(sorted_global)} - {max(sorted_global)}")

            # Check each region against the global set
            gaps_found = False
            for reg, years in sorted(region_years.items()):
                missing = global_years - years
                if missing:
                    gaps_found = True
                    missing_str = ", ".join(map(str, sorted(list(missing))))
                    print(f"   [!] GAP FOUND: {reg:<20} | Missing: {missing_str}")
                else:
                    # Optional: print OK for specific debugging if needed
                    # print(f"   [OK] {reg:<20}")
                    pass

            if not gaps_found:
                print("   [OK] No regional gaps found. All regions have consistent year sets.")
        else:
            print(f"\n[METRIC] {name.upper()} | NO DATA")


if __name__ == "__main__":
    check_detailed_availability()

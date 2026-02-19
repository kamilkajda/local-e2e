import glob
import json
import os

# --- ENVIRONMENT SETUP ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))


def run_full_opolskie_audit():
    """
    Performs a deep audit of all metrics defined in gus_metrics.json
    specifically for the region 'OPOLSKIE' and year 2023.
    Includes Attribute ID (attrId) to diagnose why values might be 0.0.
    """
    print("--- RAW DATA AUDIT: OPOLSKIE (2023) ---")

    metrics_path = os.path.join(project_root, "configs", "gus_metrics.json")
    if not os.path.exists(metrics_path):
        print(f"[!] Config file missing: {metrics_path}")
        return

    with open(metrics_path, "r", encoding="utf-8") as f:
        metrics = json.load(f)

    raw_data_path = os.path.join(project_root, "data", "raw")

    print(f"{'METRIC NAME':<45} | {'VALUE':<12} | {'ATTR'}")
    print("-" * 100)

    for metric in metrics:
        metric_name = metric["name"]
        metric_dir = os.path.join(raw_data_path, metric_name)

        if not os.path.exists(metric_dir):
            print(f"{metric_name:<45} | [!] FOLDER MISSING")
            continue

        json_files = glob.glob(os.path.join(metric_dir, "*.json"))

        region_found = False
        year_found = False
        found_val = None
        found_attr = None
        available_years = []

        for json_file in json_files:
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    for unit in data.get("results", []):
                        unit_name_clean = unit.get("name", "").upper().strip()
                        if unit_name_clean == "OPOLSKIE":
                            region_found = True
                            values = unit.get("values", [])
                            available_years = [str(v.get("year")) for v in values]

                            val_2023 = next(
                                (v for v in values if str(v.get("year")) == "2023"),
                                None,
                            )

                            if val_2023:
                                year_found = True
                                found_val = val_2023.get("val")
                                found_attr = val_2023.get("attrId")
                                break
                    if year_found:
                        break
            except Exception:
                continue

        # Reporting
        if year_found:
            status_val = str(found_val)
            print(f"{metric_name:<45} | {status_val:<12} | {found_attr}")
        elif region_found:
            years_preview = ", ".join(sorted(available_years)[-5:])
            print(f"{metric_name:<45} | [!] 2023 MISSING (Found: {years_preview}...)")
        else:
            print(f"{metric_name:<45} | [!!] OPOLSKIE NOT IN FILE")


if __name__ == "__main__":
    run_full_opolskie_audit()

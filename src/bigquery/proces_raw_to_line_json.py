import json
import os

# Path to project root — two levels up from src/bigquery/
root = os.path.join(os.path.dirname(__file__), "..", "..")

# Load config
config_path = os.path.join(root, "configs", "dev", "settings.json")
with open(config_path, "r") as f:
    config = json.load(f)
print(config_path)
print(config.get("bigquery"))


input_dir = os.path.join(root, config["paths"]["raw"])
output_dir = os.path.join(root, config["bigquery"]["bq_ready_path"])

os.makedirs(output_dir, exist_ok=True)

for folder in os.listdir(input_dir):
    folder_path = os.path.join(input_dir, folder)
    if not os.path.isdir(folder_path):
        continue
    for filename in os.listdir(folder_path):
        if not filename.endswith(".json"):
            continue
        filepath = os.path.join(folder_path, filename)
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        out_file = os.path.join(output_dir, filename.replace(".json", ".jsonl"))
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(json.dumps(data) + "\n")
        print(f"Converted: {filename}")

print("Done")

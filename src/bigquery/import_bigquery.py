import json
import os
from google.cloud import bigquery
from google.oauth2 import service_account

# Path to project root
root = os.path.join(os.path.dirname(__file__), "..", "..")

# Load config
config_path = os.path.join(root, "configs", "dev", "settings.json")
with open(config_path, "r") as f:
    config = json.load(f)

# Initialize BigQuery client with service account
credentials = service_account.Credentials.from_service_account_file(
    config["bigquery"]["credentials_path"]
)
client = bigquery.Client(project=config["bigquery"]["project"], credentials=credentials)

dataset_id = config["bigquery"]["dataset"]
input_dir = os.path.join(root, config["bigquery"]["bq_ready_path"])

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    autodetect=True,
)

for filename in os.listdir(input_dir):
    if not filename.endswith(".jsonl"):
        continue
    table_name = filename.replace(".jsonl", "")
    table_id = f"{client.project}.{dataset_id}.{table_name}"
    with open(os.path.join(input_dir, filename), "rb") as f:
        job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()
        print(f"Loaded: {table_name}")

print("Done")

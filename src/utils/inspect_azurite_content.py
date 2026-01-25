import os
import sys
import argparse

# --- ENVIRONMENT SETUP ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_root)

try:
    from azure.storage.blob import BlobServiceClient
    from src.pyspark.utils import load_config
except ImportError as e:
    print(f"\n[!] ERROR: Missing dependency or module. {e}")
    sys.exit(1)

def parse_args():
    parser = argparse.ArgumentParser(description="Inspect Azurite Content")
    parser.add_argument("--env", default="dev", choices=["dev", "prod"], help="Environment to inspect (dev/prod)")
    return parser.parse_args()

def inspect_content():
    args = parse_args()
    
    # 1. Load Configuration based on ENV argument
    config_path = os.path.join(project_root, "configs", args.env, "settings.json")
    
    if not os.path.exists(config_path):
        print(f"[!] ERROR: Config file not found at {config_path}")
        return

    print(f"--- AZURITE CONTENT INSPECTION ({args.env.upper()}) ---")
    
    config = load_config(config_path)
    storage_conf = config.get('storage', {})
    connection_string = storage_conf.get('connection_string')
    target_container = storage_conf.get('container_name') # Focus on the main data container
    
    if not connection_string:
        print("[!] ERROR: Connection string missing in settings.json")
        return

    # Extract target host for display
    target_host = "Unknown"
    if "BlobEndpoint=" in connection_string:
        target_host = connection_string.split("BlobEndpoint=")[1].split(";")[0]
    print(f"Target Host: {target_host}")

    try:
        client = BlobServiceClient.from_connection_string(connection_string, api_version="2021-08-06")
        
        print(f"Inspecting container: {target_container}")
        container_client = client.get_container_client(target_container)
        
        if not container_client.exists():
            print(f"[INFO] Container '{target_container}' does not exist.")
            return

        blobs = list(container_client.list_blobs())
        
        if not blobs:
            print("    (Empty)")
        else:
            for blob in blobs:
                size_str = f"{blob.size} B"
                if blob.size > 1024:
                    size_str = f"{blob.size / 1024:.2f} KB"
                
                print(f"    - {blob.name:<60} | {size_str}")

        print("\n[SUCCESS] Inspection complete.")

    except Exception as e:
        print(f"\n[ERROR] Could not list content: {e}")

if __name__ == "__main__":
    inspect_content()
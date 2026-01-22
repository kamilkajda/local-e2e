import sys
import os

# --- ENVIRONMENT SETUP ---
# Determine project root dynamically based on script location
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)

# If running from src/utils (2 levels deep) vs exploration (1 level deep)
if parent_dir_name in ['utils', 'pyspark']:
    project_root = os.path.dirname(os.path.dirname(current_dir))
else:
    # Default for exploration/ folder
    project_root = os.path.dirname(current_dir)

# Add project root to system path
if project_root not in sys.path:
    sys.path.append(project_root)

try:
    from azure.storage.blob import BlobServiceClient
    from src.pyspark.utils import load_config
except ImportError as e:
    print(f"\n[!] ERROR: Missing dependency or module. {e}")
    print("Ensure you are running this from the project environment.")
    print("Required: pip install azure-storage-blob")
    sys.exit(1)

def reset_azurite():
    """
    Deletes specified containers to clean the environment.
    Uses configs/dev/settings.json to determine target Azurite.
    """
    # 1. Load Configuration (DEV by default)
    config_path = os.path.join(project_root, "configs", "dev", "settings.json")
    
    if not os.path.exists(config_path):
        print(f"[!] ERROR: Config file not found at {config_path}")
        return

    print(f"--- AZURITE CLEANUP TOOL ---")
    print(f"Loading config from: {config_path}")
    
    config = load_config(config_path)
    connection_string = config.get('storage', {}).get('connection_string')
    
    if not connection_string:
        print("[!] ERROR: Connection string missing in settings.json")
        return

    # Extract target host for display
    target_host = "Unknown"
    if "BlobEndpoint=" in connection_string:
        target_host = connection_string.split("BlobEndpoint=")[1].split(";")[0]
    
    print(f"Target: {target_host}")
    
    containers_to_delete = ["etl-data", "connection-test-pc"]
    
    try:
        # Use specific API version compatible with local Azurite
        client = BlobServiceClient.from_connection_string(connection_string, api_version="2021-08-06")
        
        for container_name in containers_to_delete:
            print(f"Deleting container: {container_name}...", end=" ")
            try:
                client.delete_container(container_name)
                print("[DELETED]")
            except Exception as e:
                # 404 means it doesn't exist, which is fine
                if "404" in str(e):
                    print("[NOT FOUND - SKIPPED]")
                else:
                    print(f"\n[ERROR] {e}")

        print("\nCleanup complete.")

    except Exception as e:
        print(f"\n[CRITICAL ERROR] Could not connect: {e}")

if __name__ == "__main__":
    reset_azurite()
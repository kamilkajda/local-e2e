import os
import sys

# --- ENVIRONMENT SETUP ---
# Determine project root to import shared modules and load config
current_dir = os.path.dirname(os.path.abspath(__file__))
# Go up 2 levels: src/utils -> src -> project_root
project_root = os.path.dirname(os.path.dirname(current_dir))

# Add project root to system path to allow imports like 'src.pyspark.utils'
sys.path.append(project_root)

try:
    from azure.storage.blob import BlobServiceClient
    from src.pyspark.utils import load_config
except ImportError as e:
    print(f"\n[!] ERROR: Missing dependency or module. {e}")
    print("Ensure you are running this from the project environment.")
    sys.exit(1)

def inspect_content():
    """
    Connects to Azurite using the project's settings.json and lists content.
    """
    # 1. Load Configuration (DEV by default)
    config_path = os.path.join(project_root, "configs", "dev", "settings.json")
    
    if not os.path.exists(config_path):
        print(f"[!] ERROR: Config file not found at {config_path}")
        print("Please copy settings.template.json to settings.json first.")
        return

    print(f"--- AZURITE CONTENT INSPECTION ---")
    print(f"Loading config from: {config_path}")
    
    config = load_config(config_path)
    connection_string = config.get('storage', {}).get('connection_string')
    
    if not connection_string:
        print("[!] ERROR: Connection string missing in settings.json")
        return

    # Extract target host for display info (simple parsing)
    target_host = "Unknown"
    if "BlobEndpoint=" in connection_string:
        target_host = connection_string.split("BlobEndpoint=")[1].split(";")[0]
    print(f"Target: {target_host}")

    try:
        # Explicitly set api_version to compatible one for local Azurite
        client = BlobServiceClient.from_connection_string(connection_string, api_version="2021-08-06")
        
        print("Connecting...")
        containers = list(client.list_containers())
        
        if not containers:
             print("[INFO] Connected successfully, but NO containers found.")
             return

        found_files = False
        
        for container in containers:
            print(f"\n[Container] {container.name}")
            container_client = client.get_container_client(container.name)
            
            # List all blobs (files) in this container
            blobs = list(container_client.list_blobs())
            
            if not blobs:
                print("    (Empty)")
            else:
                found_files = True
                for blob in blobs:
                    # Convert bytes to KB/MB for readability
                    size_str = f"{blob.size} B"
                    if blob.size > 1024:
                        size_str = f"{blob.size / 1024:.2f} KB"
                    
                    print(f"    - {blob.name:<60} | {size_str}")

        if found_files:
            print("\n[SUCCESS] Files found locally. Power BI should see this structure.")
        else:
            print("\n[INFO] Containers exist but are empty.")

    except Exception as e:
        print(f"\n[ERROR] Could not list content: {e}")
        print("Check if Laptop firewall allows port 10000 and Azurite is running.")
    
    print("\n" + "="*30)
    # input("Inspection finished. Press Enter to exit...")

if __name__ == "__main__":
    inspect_content()
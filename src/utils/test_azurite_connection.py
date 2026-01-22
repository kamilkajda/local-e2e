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
    print("Required: pip install azure-storage-blob")
    sys.exit(1)

def run_diagnostics():
    """
    Performs a connectivity and write-permission test using project settings.
    """
    # 1. Load Configuration (DEV by default for testing)
    config_path = os.path.join(project_root, "configs", "dev", "settings.json")
    
    if not os.path.exists(config_path):
        print(f"[!] ERROR: Config file not found at {config_path}")
        print("Please copy settings.template.json to settings.json first.")
        return

    print(f"--- AZURITE REMOTE DIAGNOSTICS ---")
    print(f"Loading config from: {config_path}")
    
    config = load_config(config_path)
    connection_string = config.get('storage', {}).get('connection_string')
    
    if not connection_string:
        print("[!] ERROR: Connection string missing in settings.json")
        return

    # Extract target host for display info
    target_host = "Unknown"
    if "BlobEndpoint=" in connection_string:
        target_host = connection_string.split("BlobEndpoint=")[1].split(";")[0]
    
    print(f"Target Endpoint: {target_host}")
    print(f"Python Path: {sys.executable}")
    
    try:
        # Initialize the client
        # Explicitly set api_version to '2021-08-06' for local Azurite compatibility
        client = BlobServiceClient.from_connection_string(connection_string, api_version="2021-08-06")
        
        # 1. Connectivity Check (Read)
        print("Step 1: Fetching container list...")
        containers = list(client.list_containers())
        print(f"   [OK] Connection established. Found {len(containers)} containers.")
        
        # 2. Permission Check (Write)
        test_container = "connection-test-pc"
        print(f"Step 2: Verifying write access (Container: {test_container})...")
        
        existing_names = [c.name for c in containers]
        if test_container not in existing_names:
            client.create_container(test_container)
            print(f"   [OK] Created test container successfully.")
        else:
            print(f"   [INFO] Test container already exists (Write access verified previously).")

        print("\n--- CONCLUSION: SUCCESS ---")
        print("The network and authentication are working correctly.")
        print("You can now proceed with ETL jobs or Power BI integration.")

    except Exception as e:
        print(f"\n[!] DIAGNOSTICS FAILED")
        print(f"Details: {e}")
        print("\nTroubleshooting:")
        print(f"1. Verify Azurite is running on the target host.")
        print("2. Ensure Firewall on the host allows Inbound traffic on port 10000.")
        print("3. Check if settings.json has the correct IP address.")

    # Prevent terminal auto-close if run via click
    print("\n" + "="*30)
    # input("Diagnostics finished. Press Enter to exit...")

if __name__ == "__main__":
    run_diagnostics()
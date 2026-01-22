import json
import sys
import argparse

try:
    from azure.storage.blob import BlobServiceClient, PublicAccess
except ImportError:
    print("[!] ERROR: azure-storage-blob is missing. Run: pip install azure-storage-blob")
    sys.exit(1)

def load_config(config_path):
    """
    Safely loads a JSON configuration file.
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Config file not found at {config_path}")
        sys.exit(10)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in {config_path}")
        sys.exit(11)

def parse_arguments():
    """
    Parses command-line arguments for the ETL job.
    Returns the parsed arguments object.
    """
    parser = argparse.ArgumentParser(description="PySpark ETL Job")
    parser.add_argument("--config", required=True, help="Path to the environment configuration file")
    
    # Optional flag to force redownload of data
    parser.add_argument("--force", action="store_true", help="Force redownload of raw data from API")
    
    try:
        args = parser.parse_args()
        return args
    except Exception as e:
        print(f"Error parsing arguments: {e}")
        sys.exit(99)

def ensure_container_exists(connection_string, container_name):
    """
    Checks if the Azure Blob container exists, and creates it if not.
    SETS PUBLIC ACCESS to 'Container' level to allow Power BI Web Connector access.
    """
    print(f"   [Utils] Checking container '{container_name}'...")
    try:
        # Use specific API version compatible with local Azurite
        client = BlobServiceClient.from_connection_string(connection_string, api_version="2021-08-06")
        container_client = client.get_container_client(container_name)
        
        if not container_client.exists():
            # KEY CHANGE: Create with Public Access enabled (Container level)
            container_client.create_container(public_access=PublicAccess.Container)
            print(f"   [Utils] Container '{container_name}' created successfully (Public Access Enabled).")
        else:
            print(f"   [Utils] Container '{container_name}' already exists.")
            # Note: We don't modify access if it exists. Must delete container to reset.
            
    except Exception as e:
        print(f"   [Utils] Warning: Container check failed. Spark might fail later. Error: {e}")
import json
import sys
import argparse

try:
    from azure.storage.blob import BlobServiceClient, PublicAccess
except ImportError:
    print("[!] ERROR: azure-storage-blob dependency is missing. Provision the environment using requirements.txt.")
    sys.exit(1)

def load_config(config_path):
    """
    Performs safe deserialization of the JSON configuration file. 
    Handles file system exceptions and malformed JSON to prevent pipeline initialization failures.
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Critical Error: Configuration file not found at {config_path}")
        sys.exit(10)
    except json.JSONDecodeError:
        print(f"Critical Error: Failed to parse malformed JSON in {config_path}")
        sys.exit(11)

def parse_arguments():
    """
    Orchestrates command-line interface (CLI) arguments for the ETL job.
    Supports operational flags for configuration pathing and data synchronization overrides.
    """
    parser = argparse.ArgumentParser(description="PySpark ETL Orchestration")
    parser.add_argument("--config", required=True, help="Absolute or relative path to the settings.json file")
    
    # Operational override for idempotency: forces re-extraction from the source API
    parser.add_argument("--force", action="store_true", help="Force cache invalidation and redownload raw data")
    
    try:
        return parser.parse_args()
    except Exception as e:
        print(f"Argument Parsing Failure: {e}")
        sys.exit(99)

def ensure_container_exists(connection_string, container_name):
    """
    Verifies the existence of the target storage container and provisions it if absent.
    Configures the Public Access policy to 'Container' level to facilitate unauthenticated 
    read access for Power BI Web Connectors.
    """
    print(f"   [Utils] Verifying storage container state: '{container_name}'...")
    try:
        # Initialize service client with specific API version for local Azurite emulator parity
        client = BlobServiceClient.from_connection_string(connection_string, api_version="2021-08-06")
        container_client = client.get_container_client(container_name)
        
        if not container_client.exists():
            # Provision container with PublicAccess.Container to enable stable HTTP connectivity for BI tools
            container_client.create_container(public_access=PublicAccess.Container)
            print(f"   [Utils] Resource provisioned: '{container_name}' (Public Access: ENABLED).")
        else:
            print(f"   [Utils] Resource state confirmed: '{container_name}' is active.")
            
    except Exception as e:
        print(f"   [Utils] Infrastructure Warning: Container verification failed. Potential downstream I/O errors: {e}")
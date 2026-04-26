import os
from google.cloud import secretmanager

def get_secret(secret_id: str, version_id: str = "latest") -> str:
    """Access the payload for the given secret version if one exists."""
    project_id = os.environ.get("PROJECT_ID", "kabum-gcp") # Default or get from env
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

DESTINATIONS = {
        "17841402946855997": {
            "project_id": "kabum-gcp",
            "dataset_id": "meta_organic"
        }
    }   
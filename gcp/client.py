"""
client.py

Create clients for necessary services.
"""

from google.cloud import bigquery
from google.cloud import storage
from google.cloud import firestore


def get_client(service_name: str):
    """get_client

    Args:
        service_name (str): full name of application service (storage, bigquery, firestore)

    Returns:
        google.Client: client for given service
    """
    supported_clients = {
        'firestore': firestore.Client(),
        'storage': storage.Client(),
        'bigquery': bigquery.Client(),
    }
    return supported_clients[service_name]

def get_bigquery_client() -> bigquery.Client:
    return bigquery.Client()

def get_gcs_client() -> storage.Client:
    return storage.Client()

def get_firestore_client() -> firestore.Client:
    return firestore.Client()


if __name__ == "__main__":
    for service in ['firestore', 'storage', 'bigquery']:
        assert get_client(service) is not None

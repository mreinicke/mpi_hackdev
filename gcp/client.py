"""
client.py

Create clients for necessary services.
"""

from google.cloud import bigquery
from google.cloud import storage
from google.cloud import firestore


def get_bigquery_client() -> bigquery.Client:
    """[summary]

    Returns:
        bigquery.Client: [description]
    """
    return bigquery.Client()


def get_gcs_client() -> storage.Client:
    """[summary]

    Returns:
        storage.Client: [description]
    """
    return storage.Client()

def get_firestore_client() -> firestore.Client:
    """[summary]

    Returns:
        firestore.Client: [description]
    """
    return firestore.Client()

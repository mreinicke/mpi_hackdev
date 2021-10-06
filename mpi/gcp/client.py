"""
client.py

Create clients for necessary services.
"""

from mpi.settings.settings import Settings
from typing import Set
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import firestore
from google.cloud import secretmanager

import json

from google.oauth2 import service_account

import logging
logger = logging.getLogger(__name__)


def get_bigquery_client(secret=None) -> bigquery.Client:
    """[summary]

    Returns:
        bigquery.Client: [description]
    """
    return bigquery.Client(credentials=get_service_account_credentials(secret))


def get_gcs_client(secret=None) -> storage.Client:
    """[summary]

    Returns:
        storage.Client: [description]
    """
    return storage.Client(credentials=get_service_account_credentials(secret))


def get_firestore_client(secret=None) -> firestore.Client:
    """[summary]

    Returns:
        firestore.Client: [description]
    """
    return firestore.Client(credentials=get_service_account_credentials(secret))


def get_secrets_client() -> secretmanager.SecretManagerServiceClient:
    """[summary]

    Returns:
        secretmanager.SecretManagerServiceClient: [description]
    """
    return secretmanager.SecretManagerServiceClient()


def get_service_account_credentials(secret: str = None):
    if secret is None:
        config = Settings()
        secret = config.MPI_SERVICE_SECRET_NAME
    
    secrets_client = get_secrets_client()
    response = secrets_client.access_secret_version(request={'name': secret})
    service_account_info = json.loads(response.payload.data.decode("UTF-8"))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    return credentials
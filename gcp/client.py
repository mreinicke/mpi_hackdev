"""
client.py

Create clients for necessary services.
"""

from google.cloud import bigquery
from google.cloud import storage
from google.cloud import firestore
from google.cloud import secretmanager

import json

from google.oauth2 import service_account

from settings import config

import logging
logger = logging.getLogger(__name__)


def get_bigquery_client() -> bigquery.Client:
    """[summary]

    Returns:
        bigquery.Client: [description]
    """

    return bigquery.Client(credentials=get_service_account_credentials())


def get_gcs_client() -> storage.Client:
    """[summary]

    Returns:
        storage.Client: [description]
    """
    return storage.Client(credentials=get_service_account_credentials())


def get_firestore_client() -> firestore.Client:
    """[summary]

    Returns:
        firestore.Client: [description]
    """
    return firestore.Client(credentials=get_service_account_credentials())


def get_secrets_client() -> secretmanager.SecretManagerServiceClient:
    """[summary]

    Returns:
        secretmanager.SecretManagerServiceClient: [description]
    """
    return secretmanager.SecretManagerServiceClient()


def get_service_account_credentials():
    secrets_client = get_secrets_client()
    response = secrets_client.access_secret_version(request={'name': config.MPI_SERVICE_SECRET_NAME})
    service_account_info = json.loads(response.payload.data.decode("UTF-8"))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    return credentials
from google.cloud import bigquery
from gcp.client import (
    get_bigquery_client, 
    get_gcs_client, 
    get_firestore_client,
    get_secrets_client,
    get_service_account_credentials,
)

from config import GCS_BUCKET_NAME

import os

import pytest 

import logging
logger = logging.getLogger(__name__)


def test_secrets_client():
    client = get_secrets_client()
    # Access secret
    response = client.access_secret_version(request={'name': 'projects/319293654677/secrets/mpi-sa-key/versions/latest'})
    assert response.payload.data.decode("UTF-8") is not None


def test_get_service_account_credentials():
    assert get_service_account_credentials() is not None


@pytest.mark.incremental
class TestBiguqeryIO:
    def test_bigquery_read(self):
        bqclient = get_bigquery_client()
        # Perform a query.
        QUERY = (
            'SELECT * FROM `ut-dws-udrc-dev.ADHOC.GRADUATES_002D66FA-300A-4018-814F-A68E07D811A1` '
            'LIMIT 100')
        query_job = bqclient.query(QUERY)  # API request
        rows = query_job.result()  # Waits for query to finish
        assert rows is not None


    def test_bigquery_write(self):
        bqclient = get_bigquery_client()
        QUERY = (
            'CREATE TABLE IF NOT EXISTS `ut-dws-udrc-dev.ADHOC.test_table_create_hacky` '
            'AS SELECT (1) as test_col FROM (select SESSION_USER())'
        )
        query_job = bqclient.query(QUERY)
        assert query_job.result() is not None


@pytest.mark.incremental
class TestGCSIO:
    def test_gcs_client(self):
        client = get_gcs_client()
        assert client is not None

    def test_bucket_load(self):
        client = get_gcs_client()
        blobs = client.list_blobs(
            GCS_BUCKET_NAME, 
            prefix='index/udrc.png')
        with open('test_file.png', 'wb+') as file_obj:
            client.download_blob_to_file(
                f'gs://{GCS_BUCKET_NAME}/index/udrc.png',
                file_obj,
                raw_download=True,
            )

    def test_bucket_write(self):
        from google.cloud.storage import Blob
        client = get_gcs_client()
        bucket = client.get_bucket(GCS_BUCKET_NAME)
        blob = Blob("index/test-file", bucket)
        fpath = os.path.join(os.getcwd(), 'tests', 'test_assets', 'test_file.txt')
        with open(fpath, 'rb') as file_obj:
            print(blob.upload_from_file(file_obj))


@pytest.mark.incremental
class TestFirestoreIO:
    def test_firestore_load(self):
        db = get_firestore_client()
        collection = db.collection('mpi-collection')
        for doc in collection.list_documents(page_size=1):
            print(doc)

    def test_firestore_write(self):    
        import random
        db = get_firestore_client()
        collection = db.collection('mpi-collection')
        test_data = {'some_nested_array': ['some more', 'andmore'], 'val': 2}
        print(
            collection.add(test_data, str(random.randint(1000,100000)))
        )
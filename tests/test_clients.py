# test_clients.py

from gcp.client import bqclient



def create_bigquery_client():
    assert bqclient is not None
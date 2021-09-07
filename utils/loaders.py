# loaders.py


from gcp.client import get_bigquery_client

def load_bigquery_table(tablename):
    client = get_bigquery_client()
    return client.get_table(tablename)
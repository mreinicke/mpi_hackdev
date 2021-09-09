# loaders.py


from gcp.client import get_bigquery_client

def load_bigquery_table(tablename):
    client = get_bigquery_client()
    return client.get_table(tablename)


## Load data into digestable forms
def create_generator_from_iterators(*args):
    for iterator in args:
        for member in iterator:
            yield member
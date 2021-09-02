from google.cloud import bigquery
from gcp.client import get_client


if __name__ == '__main__':

    def test_bigquery_read():
        bqclient = get_client('bigquery')
        # Perform a query.
        QUERY = (
            'SELECT * FROM `ut-dws-udrc-dev.ADHOC.GRADUATES_002D66FA-300A-4018-814F-A68E07D811A1` '
            'LIMIT 100')
        query_job = bqclient.query(QUERY)  # API request
        rows = query_job.result()  # Waits for query to finish
        assert rows is not None

    def test_bucket_load():
        client = get_client('storage')
        bucket = client.get_bucket()
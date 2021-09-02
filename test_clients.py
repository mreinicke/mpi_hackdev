from google.cloud import bigquery
from gcp.client import get_client, get_gcs_client, get_firestore_client


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


    def test_bigquery_write():
        bqclient = get_client('bigquery')
        QUERY = (
            'CREATE TABLE IF NOT EXISTS `ut-dws-udrc-dev.ADHOC.test_table_create_hacky` '
            'AS SELECT (1) as test_col FROM (select SESSION_USER())'
        )
        query_job = bqclient.query(QUERY)
        print(query_job.result())


    def test_bucket_load():
        client = get_gcs_client()
        blobs = client.list_blobs(
            'hackathon-mpi-bucket', 
            prefix='index/udrc.png')
        with open('test_file.png', 'wb+') as file_obj:
            client.download_blob_to_file(
                'gs://hackathon-mpi-bucket/index/udrc.png',
                file_obj,
                raw_download=True,
            )


    def test_bucket_write():
        from google.cloud.storage import Blob
        client = get_gcs_client()
        bucket = client.get_bucket('hackathon-mpi-bucket')
        blob = Blob("index/test-file", bucket)
        with open('README.md', 'rb') as file_obj:
            print(blob.upload_from_file(file_obj))


    def test_firestore_load():
        db = get_firestore_client()
        collection = db.collection('mpi-collection')
        for doc in collection.list_documents(page_size=1):
            print(doc)


    def test_firestore_write():    
        import random
        db = get_firestore_client()
        collection = db.collection('mpi-collection')
        test_data = {'some_nested_array': ['some more', 'andmore'], 'val': 2}
        print(
            collection.add(test_data, str(random.randint(1000,100000)))
        )


    test_bigquery_read()
    test_bigquery_write()
    test_bucket_load()
    test_bucket_write()
    test_firestore_load()
    test_firestore_write()
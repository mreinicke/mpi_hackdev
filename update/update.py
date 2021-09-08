"""
Update

Post processing and assignment after MPI classification is completed.
    1. Assigning new MPIs
    2. Updating MPI pool
    3. Rebuilding MPI vectors table
    4. Rebuilding search indexes
"""

from config import FIRESTORE_IDENTITY_POOL

from google.cloud.bigquery.job.query import QueryJob
from google.cloud.firestore_v1 import collection
from utils.loaders import load_bigquery_table
from utils.runners import send_query

from gcp.client import get_firestore_client

#############################
### 1. Assignign New MPIs ###
#############################

def update_preprocessed_table(tablename: str) -> tuple:
    err = None
    
    # Try to load table
    try:
        table = load_bigquery_table(tablename)
    except Exception as e:
        return e, tablename

    QUERY = f"""
    UPDATE `{tablename}`
    SET 
        mpi = GENERATE_UUID(),
        prob_match = 1.0
    WHERE mpi is NULL
    """

    err, _ = send_query(QUERY)
    return err, tablename


def update_mpi_pool_from_table(tablename: str, guid: str) -> tuple:
    client = get_firestore_client()
    col = client.collection(FIRESTORE_IDENTITY_POOL)



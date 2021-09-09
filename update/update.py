"""
Update

Post processing and assignment after MPI classification is completed.
    1. Assigning new MPIs
    2. Updating MPI pool
    3. Rebuilding MPI vectors table
    4. Rebuilding search indexes
"""

from google.cloud import bigquery
from config import FIRESTORE_IDENTITY_POOL

from google.cloud.firestore_v1 import collection
from utils.loaders import load_bigquery_table
from utils.runners import send_query

from gcp.client import get_firestore_client
from gcp.models import NoSQLSerializer

import logging
logger = logging.getLogger(__name__)

##########################
### 1. Assign New MPIs ###
##########################

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



##########################
### 2. Update MPI Pool ###
##########################

def update_mpi_pool_from_table(tablename: str, guid: str) -> tuple:
    client = get_firestore_client()
    col = client.collection(FIRESTORE_IDENTITY_POOL)


def get_rows_from_table(tablename: str) -> bigquery.table.RowIterator:
    query = f"SELECT * FROM `{tablename}`"
    err, rows = send_query(query)
    if err is None:
        return rows
    else:
        raise err


def serialize_rows_from_table(tablename: str, guid: str) -> tuple:
    s = NoSQLSerializer(context={'guid':})
    rows = get_rows_from_table(tablename)
    return [s()]
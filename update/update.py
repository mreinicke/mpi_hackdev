"""
Update

Post processing and assignment after MPI classification is completed.
    1. Assigning new MPIs
    2. Updating MPI pool
    3. Rebuilding MPI vectors table
    4. Rebuilding search indexes
"""

from google.cloud import bigquery
from google.cloud import firestore
from config import FIRESTORE_IDENTITY_POOL

from google.cloud.firestore_v1 import collection, base_document
from utils.loaders import load_bigquery_table, create_generator_from_iterators
from utils.runners import send_query, QueueJobHander

from gcp.client import get_firestore_client
from gcp.models import NoSQLSerializer, Context

from queue import Queue

import logging
logger = logging.getLogger(__name__)

##########################
### 1. Assign New MPIs ###
##########################

def update_preprocessed_table(context: Context) -> tuple:
    """Update Preprocessed Table
    
        Genereate MPI for unmatched rows.  Set prob_match for those rows to 1.
    """
    err = None
    tablename = context.source_tablename
    # Try to load table
    try:
        load_bigquery_table(tablename)
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

def update_firestore_from_table(context: Context, tablename=None, num_threads=2) -> tuple:
    if tablename is None:
        tablename = context.source_tablename
    # Initialize a serializer
    serializer = NoSQLSerializer(context=context)

    # Create the serializer function 
    def _infn(sequence = None, queue: Queue = None, serializer = serializer) -> dict:
        for m in sequence:
            if m == 'done':  ## allow completion message to be queued by infn
                queue.put(m)
            else:
                queue.put(serializer(dict(m)).as_dict())
        return 'complete'

    # Create the firestore write/update function
    client = get_firestore_client()
    def _outfn(*args, queue: Queue = None, client=client, **kwargs):
        while True:
            if queue.not_empty:
                m = queue.get()
                if m != 'done':
                    push_row_to_firestore(m, client)
                else:
                    queue.task_done()
                    break
                queue.task_done()
        return 'complete'

    # Combine the row iterator with a bunch of completion messages to stop all the threads
    sequence = create_generator_from_iterators(
        get_rows_from_table(tablename=tablename),
        ["done"]*(num_threads+1)
    )

    # Initialize the queue/thread handler
    handler = QueueJobHander(
        infn=_infn,
        outfn=_outfn,
        sequence=sequence
    )
    in_res, out_res = handler.run()


def get_rows_from_table(tablename=None) -> bigquery.table.RowIterator:
    query = f"SELECT * FROM `{tablename}`"
    err, rows = send_query(query)
    if err is None:
        return rows
    else:
        raise err


def serialize_rows_from_table(context: Context, tablename=None) -> tuple:
    if tablename is None:
        tablename = context.source_tablename
    s = NoSQLSerializer(context=context)
    rows = get_rows_from_table(tablename=tablename)
    return tuple([s(dict(r)).as_dict() for r in rows])


def push_row_to_firestore(row: dict, client: firestore.Client):
    def _get_doc(doc_id: str, col: collection) -> base_document:
        return col.document(doc_id)

    def _get_guid_index_in_sources(sources: list, guid: str) -> int:
        guid_history = [i for i, s in enumerate(sources) if s['guid']==guid]
        if len(guid_history) > 0:
            return guid_history[0]
        else:
            return None

    def _replace_source_at_index(sources, guid_index) -> list:
        if guid_index is None:
            sources.append(row['sources'][0])
        else:
            sources[guid_index] = row['sources'][0]
        return sources

    col = client.collection(FIRESTORE_IDENTITY_POOL)
    doc_id = row.pop('mpi')  # Assign MPI to document ID
    guid = row['sources'][0]['guid']
    document = _get_doc(doc_id, col)
    document_snapshot = document.get()

    if document_snapshot.exists:
        sources = document_snapshot.get('sources')
        guid_index = _get_guid_index_in_sources(sources, guid)
        sources = _replace_source_at_index(sources, guid_index)
        document.update({'sources': sources})
        # logger.debug(f'Updated document {doc_id} with {sources}')  ## Heavy logging.  Only enable if necessary.
    else:
        col.add(row, doc_id)


def push_rows_to_firestore(rows: tuple):  ## Deprecated for multi-threaded approach above
    client = get_firestore_client()
    for row in rows:
        push_row_to_firestore(row, client)
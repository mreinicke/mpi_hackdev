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
from config import FIRESTORE_IDENTITY_POOL, MPI_VECTORS_TABLE

from google.cloud.firestore_v1 import collection, base_document
from utils.loaders import load_bigquery_table, create_generator_from_iterators
from utils.runners import send_query, QueueJobHander
from utils.batch import Batch

from gcp.client import get_bigquery_client, get_firestore_client
from gcp.models import NoSQLSerializer, Context

from queue import Queue

import logging
logger = logging.getLogger(__name__)


##########################
### 2. Update MPI Pool ###
##########################

def update_firestore_from_table(context: Context, tablename=None, num_threads=2) -> tuple:
    if tablename is None:
        tablename = context.source_tablename
    # Initialize a serializer
    serializer = NoSQLSerializer(context=context)

    # Create the serializer function 
    def _infn(sequence = None, queue: Queue = None, serializer = serializer, context=context) -> dict:
        for m in sequence:
            m = next(sequence)
            logger.debug(f"message infn: {m}")
            if m == 'done':  ## allow completion message to be queued by infn
                queue.put(m)
            else:
                queue.put(serializer(dict(m)).as_dict())
        return 'complete'

    # Create the firestore write/update function
    client = get_firestore_client()
    def _outfn(*args, queue: Queue = None, client=client, context=context, **kwargs):
        rowbatch = Batch(max_size=450, filterfn=mpi_exists)
        while True:
            if queue.not_empty:
                m = queue.get()
                if m != 'done':
                    if rowbatch.add(m):
                        pass
                    else:
                        filter_run_batch(batch=rowbatch, client=client, context=context)
                        rowbatch.add(m)  ## Add returns false if max batch size has been reached.  Must run to flush batch.
                else:
                    filter_run_batch(batch=rowbatch, client=client, context=context)
                    queue.task_done()
                    break
                queue.task_done()
        return 'complete'

    # Combine the row iterator with a bunch of completion messages to stop all the threads
    sequence = create_generator_from_iterators(
        get_rows_from_table(tablename=tablename),  ## TODO: pc.*, ex.exist FROM preprocessed_classified pc LEFT JOIN (SELECT mpi, (1) AS exists FROM mpi_vectors) ex.  One query for the whole operation.  
        ["done"]*(num_threads+1)
    )

    # Initialize the queue/thread handler
    handler = QueueJobHander(
        infn=_infn,
        outfn=_outfn,
        sequence=sequence,
        num_threads=3,
        out_threads_max=2,
    )
    handler.run()




def batch_add_documents(rows: tuple, client: firestore.Client, context: Context = None) -> bool:
    """batch_add_documents

        Create document reference objects for array of MPIRecord.as_dict() data.
        Commits all records without checking existance. Batch failes as a group.
    
    Args:
        rows (tuple): [description]
        client (firestore.Client): [description]

    Returns:
        bool: True if commit successful.  Fail as a batch.
    """
    if len(rows) < 1:
        return
    logger.debug('Beginning batch add.')

    batch = client.batch()
    col = client.collection(FIRESTORE_IDENTITY_POOL)
    batch_size = 0

    for row in rows:
        doc_id = row.pop('mpi')
        doc_ref = col.document(doc_id)
        batch.set(doc_ref, row)
        batch_size += 1
    
    try:
        batch.commit()
        logger.info(f"Committed batch of {batch_size} records")
        return True
    except Exception as e:
        logger.error(e)
        return False




@firestore.transactional
def batch_update_documents(transaction: firestore.Transaction, client: firestore.Client, rows: tuple, context: Context = None) -> bool:
    """batch_update_documents

    Args:
        rows (tuple): [description]
        tranaction (firestore.Transaction): [description]

    Returns:
        bool: True if commit successful.  Fail as batch.
    """
    def _generate_doc_ref(row: dict, col: firestore.CollectionReference) -> firestore.DocumentReference:
        mpi = row['mpi']
        return col.document(mpi)

    def _get_guid_index_in_sources(sources: list, guid: str) -> int:
        guid_history = [i for i, s in enumerate(sources) if s['guid']==guid]
        if len(guid_history) > 0:
            return guid_history[0]
        else:
            return None

    def _replace_source_at_index(sources, new_source, guid_index) -> list:
        if guid_index is None:
            sources.append(new_source)
        else:
            sources[guid_index] = new_source
        return sources

    # Circumvent all if no rows
    if len(rows) < 1:
        return
    logger.debug('Beginning Batch Update')

    # Create a transaction constructor
    transaction = client.transaction()
    # Create document references (lazy) for each row
    doc_refs = [_generate_doc_ref(row) for row in rows]
    # Fetch each document's data via transaction
    snapshots = [ref.get(transaction=transaction) for ref in doc_refs]
    # Iterate through each transaction and update sources
    for i, snapshot in enumerate(snapshots):
        current_sources = snapshot.get('sources')
        new_source = rows[i]['sources'][0]
        guid_index = _get_guid_index_in_sources(sources, context.guid)
        sources = _replace_source_at_index(current_sources, new_source, guid_index)
        transaction.update(doc_refs[i], {'sources': sources})




def filter_run_batch(batch: Batch, client: firestore.Client, context: Context):
    rows_to_add, rows_to_update = batch.filter('add', 'update')
    batch_add_documents(
        rows=rows_to_add, 
        client=client,
        context=context
    )
    batch_update_documents(
        transaction=client.transaction(),
        rows=rows_to_update,
        client=client,
        context=context,
    )
    batch.flush()




def mpi_exists(rows, *args):
    def _check_mpis_in_vectors_table(rows: tuple) -> tuple:
        mpis = [f"'{row['mpi']}'" for row in rows]
        QUERY = f"""
        SELECT mpi FROM `{MPI_VECTORS_TABLE}`
        WHERE mpi in ({",".join(mpis)})"""
        err, res = send_query(QUERY)
        if err is None:
            return tuple([dict(r)['mpi'] for r in res if r is not None])
        raise(err)
    # Query batch of rows to see which MPIs have vectors (are represented in pool)
    existing_mpis = _check_mpis_in_vectors_table(rows)
    # Sort the rows by whether there are existing mpi records and tag each group
    filtered = {
        'add':tuple([row for row in rows if row['mpi'] not in existing_mpis]),
        'update':tuple([row for row in rows if row['mpi'] in existing_mpis]),
    }

    # Check the args for valid ordering.
    for arg in args:
        assert arg in filtered.keys(), f'Invalid order. Must be of (rows, add, update) or (rows, update, add)'

    return [filtered[val] for val in args]



## TODO: This iterator is not working in combined iterator context.  
##  Not all rows get returned all of the time via the returned RowIterator
##  Further exploration into iterator behavior required.
def get_rows_from_table(tablename=None, verbose=True) -> bigquery.table.RowIterator:
    query = f"SELECT * FROM `{tablename}`"
    err, rows = send_query(query, verbose=verbose)
    if err is None:
        return rows
    else:
        raise err




def serialize_rows_from_table(context: Context, tablename=None) -> tuple:  ## Deprecated for multi-threaded serialization
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
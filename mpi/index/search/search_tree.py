"""
Search Tree

Functions and pipeline wrappers to update search
reference tables and build search trees for indexing.
"""

from typing import Tuple
from google.cloud import bigquery
from google.cloud import storage

import numpy as np
from sklearn.neighbors import BallTree
import pickle
import tempfile
from typing import Tuple

from ...utils.runners import send_query
from ...settings import config

import logging
logger = logging.getLogger(__name__)

############################
### Building Search Tree ###
############################

def get_name_match_vectors(
    mpi_vectors_table: str = config.MPI_VECTORS_TABLE, 
    client: bigquery.Client = None, 
    tree_ref_table: str = config.INDEX_TREE_REF_TABLE):
    """
    Retrieve the deserialized distinct mpi-vector combinations
    from the mpi_vectors table. Because ordering is critical
    in maintaining downstream lookups, retrieving the vectors 
    will also update the tree_ref_table used in reverse lookup from 
    the nearest neighbor search.
        

    Args:
        mpi_vectors_table (str, optional): [description]. Defaults to config.MPI_VECTORS_TABLE.
        client (bigquery.Client, optional): [description]. Defaults to None.
        tree_ref_table (str, optional): [description]. Defaults to config.INDEX_TREE_REF_TABLE.
    """

    def _de_serialize_vec(vec: np.str):
        return np.array([float(x) for x in np.char.split([vec], sep="-")[0]])

    def _delete_tree_ref_table():
        logger.info("truncating tree reference table")
        query = f"TRUNCATE TABLE `{tree_ref_table}`"
        err, _ = send_query(query=query, client=client, verbose=True)
        assert err is None, err

    def _populate_tree_ref_table():
        logger.info('populating tree reference table')
        query = f"""
        INSERT INTO `{tree_ref_table}`
            (mpi, name_match_vector, index)
        SELECT 
            mpi, 
            name_match_vector,
            ROW_NUMBER() OVER() AS index
        FROM
            (SELECT DISTINCT mpi, name_match_vector FROM `{mpi_vectors_table}`)
        """
        err, _ = send_query(query, client=client, verbose=True)
        assert err is None, err

    def _retrieve_ordered_vectors():
        logger.info('retrieving tree reference data')
        query = f"""
        SELECT
            index,
            name_match_vector
        FROM
            `{tree_ref_table}`
        """
        err, res = send_query(query, client=client, verbose=True)
        assert err is None, err
        ordered_vectors = np.array(
            sorted(
                [(row['index'], row['name_match_vector']) for row in res], 
                key=lambda x: x[0]
                )
        )
        ordered_vectors = ordered_vectors[:, 1]
        return None, np.array([_de_serialize_vec(x) for x in ordered_vectors])

    # Prepare reference data for tree
    _delete_tree_ref_table()
    _populate_tree_ref_table()
    # Return de-serialized vectors ordered by reference index
    return _retrieve_ordered_vectors()



def build_tree_from_vectors(vectors: np.ndarray) -> BallTree:
    def _get_leaf_size(x: int) -> int:
        return int(np.log(x))
    logger.info(f'building search tree')
    return BallTree(
        vectors,
        leaf_size=_get_leaf_size(len(vectors))
    )


def pickle_save_tree(
    tree, 
    gcs_client: storage.Client, 
    bucketname: str = config.GCS_BUCKET_NAME,
    filename: str="index/search_tree.pkl") -> Tuple[Exception, str]:
    logger.info('saving search tree')
    s = pickle.dumps(tree)
    bucket = gcs_client.get_bucket(bucketname)
    try:
        blob = storage.Blob(filename, bucket)
        return None, blob.upload_from_string(s)
    except Exception as e:
        logger.error(e)
        return e, None


#################################
### Load Search Tree From GCS ###
#################################

def load_unpickle_tree(
    gcs_client: storage.Client, 
    bucket: str = config.GCS_BUCKET_NAME,
    filename: str = "index/search_tree.pkl") -> Tuple[Exception, object]:
    logger.info('loading search tree')
    tp = tempfile.TemporaryFile()
    try:
        gcs_client.download_blob_to_file(
            f'gs://{bucket}/{filename}',
            tp,
            raw_download=True,
        )
        tp.seek(0)
        tree = pickle.load(tp)
    except Exception as e:
        logger.error(e)
        return e, None
    tp.close()
    return None, tree


#############################
### Search and Index MPIs ###
#############################


def search_for_neighbor_mpis(
    vectors: np.ndarray, 
    tree, 
    ref_table: str, 
    query_radius=config.INDEX_NAME_NEIGHBOR_THRESHOLD,
    bigquery_client=None) -> np.ndarray:
    
    rownums = vectors[:, 0]
    emb = [x for x in vectors[:, 1]]
    ind = tree.query_radius(emb, r=query_radius)
    return query_indices_from_reference_table(
        row_indices=zip(rownums, ind), 
        ref_table=ref_table, 
        bigquery_client=bigquery_client)

    

def query_indices_from_reference_table(row_indices: np.ndarray, ref_table: str, bigquery_client: bigquery.Client) -> list:
    
    def _build_query_for_row(row_index: tuple) -> str:
        # Note the +1 on tree index to bigquery index.  the bigquery index starts at 1
        # due to it's being created with ROW_NUMBER().
        return f"""
        (
        SELECT 
            {row_index[0]} AS rownum,
            mpi
        FROM
            `{ref_table}`
        WHERE
            index IN ({','.join([str(i+1) for i in row_index[1]])})
        )
        """

    def _build_query_from_row_indices() -> str:
        SELECT = """
        SELECT 
            rownum, mpi 
        FROM 

        """
        UNIONS = " UNION DISTINCT ".join([_build_query_for_row(ri) for ri in row_indices])
        return SELECT + UNIONS

    query = _build_query_from_row_indices()
    err, res = send_query(query=query, client=bigquery_client)
    assert err is None, err
    return [tuple([r['rownum'], r['mpi']]) for r in res]
"""Index

Find candidate matches for each row where available.
"""

"""Index

    Search nearest neighbors and blocked candidates for later comparison
    and classification.


    name_index <= SearchTree(first_name+last_name)
        *middle name not indexed
        *first name/last name will not be searched 

"""
from google.cloud.firestore_v1.base_collection import BaseCollectionReference
from gcp.client import get_firestore_client
from utils.iterators import coalesce
from utils.runners import send_query

import numpy as np
from sklearn.neighbors import BallTree

from typing import List

from settings import config
import logging

logger = logging.getLogger(__name__)



class Indexer():
    def __init__(self, mapped_columns: List[str], mpi_vectors_table=None, preprocessed_table = None, secret=None) -> None:
        self.mapped_columns = mapped_columns
        self.secret = coalesce(secret, config.MPI_SERVICE_SECRET_NAME)
        self.mpi_vectors_table = coalesce(mpi_vectors_table, config.MPI_VECTORS_TABLE)
        self.preprocessed_table = coalesce(preprocessed_table, config.BIGQUERY_TEST_PREPROCESSED_TABLE)

        assert self.secret is not None, 'Must provide secret'
        assert self.mpi_vectors_table is not None, 'Must provide mpi_vectors_table'
        assert self.preprocessed_table is not None, 'Must provide preprocessed_table'

    def assemble_search(self):
        raise NotImplementedError('must implement method')

    def index(self):
        raise NotImplementedError('must implement method')



class BlockIndexer(Indexer):
    """
    Block Indexer
    
    Assembles queries to screen entire table on exact matches given blocked_index_allow 
    flag.
    """
    def __init__(self, mapped_columns: List[str], **kwargs) -> None:
        super().__init__(mapped_columns, **kwargs)
        self.block_index_allow = [
            'ssn', 'ssid', 'birth_date', 'ushe_student_id',
            'usbe_student_id', 'ustc_student_id'
        ]
        self.template_column_query = """
        SELECT
            pt.rownum,
            mt.mpi
        FROM
            `<preprocessed_table>` pt
            INNER JOIN `<mpi_vectors_table>` mt
                ON <column_name> = <column_name>
        """


    def assemble_search(self) -> str:
        def _wrap(s: str) -> str:
            return f"({s})"
        subqueries = []
        components = {}
        blocked_columns = [c for c in self.mapped_columns if c in self.block_index_allow]
        for col in blocked_columns:
            components['<column_name>'] = col
            components['<mpi_vectors_table>'] = self.mpi_vectors_table
            components['<preprocessed_table>'] = self.preprocessed_table
            subqueries.append(
                replace_components_in_query(
                    query=self.template_column_query, 
                    components=components
                )
            )
        subqueries = [_wrap(q) for q in subqueries]
        SELECT = "SELECT * FROM "
        FROM = ' UNION '.join(subqueries)
        return SELECT + FROM


    def index(self, tablename: str):
        return self.search(tablename)


# Utilities to assist in loading index assets and setup
def replace_components_in_query(query: str, components: dict) -> str:
    for c in components.keys():
        query = query.replace(c, components[c])
    return query


def get_search_tree():
    pass


def get_default_collection() -> BaseCollectionReference:
    db = get_firestore_client()
    ## TODO: does collection exist?
    return db.collection(config.FIRESTORE_IDENTITY_POOL)


def is_collection_length_large(minsize=10) -> bool:
    """Run a limited query to make sure there are some identities in the pool"""
    collection = get_default_collection()
    counts = [1 for d in collection.where('frequency_score',  '>', 0.1).limit(minsize).stream()]
    return sum(counts) >= minsize


def check_cold_start():
    return is_collection_length_large()


############################
### Building Search Tree ###
############################

def get_name_match_vectors(mpi_vectors_table: str = None):
    def _de_serialize_vec(row):
        return np.array([float(x) for x in row['name_match_vector'].split('-')])

    query = f"""
    SELECT DISTINCT name_match_vector FROM `{mpi_vectors_table}`
    """
    err, res = send_query(query)
    if err is not None:
        raise err
    return np.array([_de_serialize_vec(row) for row in res])


def build_tree_from_vectors(
    vectors: np.ndarray,
    leaf_size=2) -> BallTree:
    return BallTree(
        vectors,
        leaf_size=leaf_size
    )
    

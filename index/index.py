"""Index

    Search nearest neighbors and blocked candidates for later comparison
    and classification.


    name_index <= SearchTree(first_name+last_name)
        *middle name not indexed
        *first name/last name will not be searched

    Search Order:
        1. Block Index -> temp_table
        2. name_index -> temp_table

"""
from google.cloud.firestore_v1.base_collection import BaseCollectionReference
from gcp.client import get_bigquery_client, get_firestore_client, get_gcs_client
from utils.iterators import coalesce
from utils.runners import send_query
from utils.embeds import AlphabetVectorizer

from typing import List, Tuple
import numpy as np

from index.search.search_tree import load_unpickle_tree, search_for_neighbor_mpis

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
    def __init__(
        self,
        client,
        mapped_columns: List[str], 
        blocked_columns=config.BLOCKED_COLUMNS,
        **kwargs) -> None:

        super().__init__(mapped_columns, **kwargs)
        self.block_index_allow = blocked_columns
        self.template_column_query = """
        SELECT
            pt.rownum,
            mt.mpi
        FROM
            `<preprocessed_table>` pt
            INNER JOIN `<mpi_vectors_table>` mt
                ON CAST(pt.<column_name> AS STRING) = CAST(mt.<column_name> AS STRING)
        """
        self.client = client


    @property
    def need_run(self):
        run_intersection = set(self.block_index_allow).intersection(set(self.mapped_columns))
        if len(run_intersection) > 0:
            return True
        return False


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
        output_tablename = self.preprocessed_table + '_index'
        CREATE = f"CREATE TABLE IF NOT EXISTS `{output_tablename}` AS "
        SELECT = "SELECT * FROM "
        FROM = " UNION DISTINCT ".join(subqueries)
        return CREATE + SELECT + FROM


    def run_index(self):
        query = self.assemble_search()
        return send_query(query=query, verbose=True, client=self.client)

    def index(self):
        if self.need_run:
            return self.run_index()



class NameIndexer(Indexer):

    def __init__(
        self, 
        mapped_columns: List[str], 
        mpi_vectors_table=None,
        preprocessed_table=None,
        secret=None,
        bucketname=config.GCS_BUCKET_NAME,
        search_tree_filename="index/search_tree.pkl",
        search_tree_ref_table=config.INDEX_TREE_REF_TABLE,
        ) -> None:

        super().__init__(
            mapped_columns, mpi_vectors_table=mpi_vectors_table, 
            preprocessed_table=preprocessed_table, secret=secret)

        self.bucketname = bucketname
        self.search_tree_filename = search_tree_filename
        self.search_tree_ref_table = search_tree_ref_table
        self.bigquery_client = get_bigquery_client(secret=secret)
        self.__clf = None


    @property
    def need_run(self):
        if ('first_name' in self.mapped_columns) and ('last_name' in self.mapped_columns):
            return True
        return False


    @property
    def vectorizer(self):
        if self.__clf is None:
            self.__clf = AlphabetVectorizer()
        return self.__clf


    def assemble_search(self):
        self.tree = load_unpickle_tree(
            gcs_client=get_gcs_client(secret=self.secret),
            bucketname=self.bucketname,
            filename=self.search_tree_filename,
        )


    def index(self, rows: list) -> np.ndarray:
        """index name vectors

        Takes an array of rows[row1, row2] and returns
        an array of indices for each vector [[mpi1, mpi2], [mpi3, mpi4]].
        Meant to be used in batch context to reduce the number of times the tree
        needs to be loaded and searched.
        
        Args:
            vectors (np.ndarray): Array of name_match_vectors

        Returns:
            np.ndarray: [description]
        """
        def _concat_first_last_name(row) -> Tuple[int, str]:
            return np.array(
                (row['rownum'], row['first_name'] + row['last_name'])
            )
        def _vectorize_concat_name(row_val: tuple, vectorizer: AlphabetVectorizer) -> Tuple[int, np.ndarray]:
            return (row_val[0], vectorizer(row_val[1]))

        if self.need_run:
            self.assemble_search()
            vectors = np.array(
                [
                    _vectorize_concat_name(
                        _concat_first_last_name(row), vectorizer=self.vectorizer
                        ) for row in rows
                ]
            )
            assert len(vectors) == len(rows), \
                f'mismatch between num vectors {len(vectors)} and num rows{len(rows)}'
            return search_for_neighbor_mpis(
                vectors=vectors, 
                tree=self.tree, 
                ref_table=self.tree_ref_table,
                bigquery_client=self.bigquery_client
            )





# Utilities to assist in loading index assets and setup
def replace_components_in_query(query: str, components: dict) -> str:
    for c in components.keys():
        query = query.replace(c, components[c])
    return query


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
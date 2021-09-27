# test_index.py

## NOTE: Tests are not production safe.  They may destroy, empty, or replace tables.

from utils.runners import send_query
from gcp.client import get_bigquery_client, get_gcs_client

import pytest 
from typing import Tuple
import numpy as np

from utils.embeds import AlphabetVectorizer

from index.index import (
    BlockIndexer,
)

from index.search.search_tree import (
    get_name_match_vectors,
    build_tree_from_vectors,
    load_unpickle_tree,
    pickle_save_tree,
    search_for_neighbor_mpis
)

import logging
logger = logging.getLogger(__name__)

from settings import config


@pytest.fixture
def bigquery_client():
    from gcp.client import get_bigquery_client
    client = get_bigquery_client()
    return client


def test_block_indexer_query_assembles():
    bi = BlockIndexer(
        mapped_columns=['ssn', 'first_name', 'ssid'], 
        client=None
    )
    query = bi.assemble_search()
    logger.info(query)
    assert 'first_name' not in query


def test_block_indexer_need_run():
    bi = BlockIndexer(
        mapped_columns=['ssn', 'first_name', 'ssid'], 
        client=None
    )
    assert bi.need_run == True
    bi = BlockIndexer(
        mapped_columns=['first_name', 'last_name', 'birth_date'], 
        client=None
    )
    assert bi.need_run == False


def test_block_indexer_index(bigquery_client):
    bi = BlockIndexer(
        mapped_columns=['ssn'],   ## This will fail if mapped columns don't match actual test table
        client=bigquery_client
    )
    err, _ = bi.index()
    assert err is None



@pytest.mark.incremental
class TestTreeConstructionUsage:


    def test_get_name_match_vectors(self):
        err, nvects = get_name_match_vectors(mpi_vectors_table=config.MPI_VECTORS_TABLE)
        assert err is None
        for v in nvects:
            assert len(v) == 26  ## Alphabet vectorizer must have 26


    def test_build_use_tree(self):
        err, nvects = get_name_match_vectors(mpi_vectors_table=config.MPI_VECTORS_TABLE)
        assert err is None
        tree = build_tree_from_vectors(nvects)
        dist, ind = tree.query(nvects[0].reshape(1, -1), k=3)
        assert len(dist) == len(ind)
        assert len(dist) > 0


    def test_pickle_save_tree(self):
        _, nvects = get_name_match_vectors(mpi_vectors_table=config.MPI_VECTORS_TABLE)
        tree = build_tree_from_vectors(nvects)
        gcs_client = get_gcs_client()
        pickle_save_tree(tree, gcs_client=gcs_client)

    
    def test_load_unpickle_tree(self):
        gcs_client = get_gcs_client()
        err, tree = load_unpickle_tree(gcs_client=gcs_client)
        assert tree is not None


    def test_search_tree(self):
        def _concat_first_last_name(row) -> Tuple[int, str]:
            return np.array(
                (row['rownum'], row['first_name'] + row['last_name'])
            )
        def _vectorize_concat_name(row_val: tuple, vectorizer: AlphabetVectorizer) -> Tuple[int, np.ndarray]:
            return (row_val[0], vectorizer(row_val[1]))

        gcs_client = get_gcs_client()
        _, tree = load_unpickle_tree(gcs_client=gcs_client)

        query = f"SELECT * FROM `{config.BIGQUERY_TEST_PREPROCESSED_TABLE}`"
        _, res = send_query(query)
        rows = [r for r in res]
        vectorizer = AlphabetVectorizer()
        vectors = np.array([
            _vectorize_concat_name(_concat_first_last_name(row), vectorizer=vectorizer) 
            for row in rows])
        assert len(vectors) == len(rows), \
            f'mismatch between num vectors {len(vectors)} and num rows{len(rows)}'
        
        row_mpis = search_for_neighbor_mpis(
            vectors=vectors, 
            tree=tree, 
            ref_table=config.INDEX_TREE_REF_TABLE,
            bigquery_client=get_bigquery_client()
        )
        assert len(row_mpis) >= len(rows)

        

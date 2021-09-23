# test_index.py

import pytest 

from index.index import BlockIndexer, get_name_match_vectors, build_tree_from_vectors

import logging
logger = logging.getLogger(__name__)

from settings import config

def test_block_indexer_query_assembles():
    bi = BlockIndexer(['ssn', 'first_name', 'ssid'])
    query = bi.assemble_search()
    logger.info(query)
    assert 'first_name' not in query


@pytest.mark.incremental
class TestTreeConstructionUsage:

    def test_get_name_match_vectors(self):
        nvects = get_name_match_vectors(mpi_vectors_table=config.MPI_VECTORS_TABLE)
        for v in nvects:
            assert len(v) == 26  ## Alphabet vectorizer must have 26

    def test_build_use_tree(self):
        nvects = get_name_match_vectors(mpi_vectors_table=config.MPI_VECTORS_TABLE)
        tree = build_tree_from_vectors(nvects)
        logger.info(tree.get_n_calls())
        dist, ind = tree.query(nvects[:1], k=3)
        logger.info(nvects[:1])
        logger.info(dist)
        logger.info(ind)
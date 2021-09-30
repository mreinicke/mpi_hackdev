# test_compare.py

from comparison.comparison import Comparator
from comparison.sql import build_select_distinct_ind, build_join_preprocessed
import logging

logger = logging.getLogger(__name__)


def test_build_select_distinct_ind():
    query = build_select_distinct_ind('some_test_table')
    assert len(query) > 0


def test_build_join_preprocessed():
    query = build_join_preprocessed(tablename='some_test_table', mpi_vectors_table='mpi_vects')
    assert len(query) > 0


def test_comparator_assemble():
    c = Comparator(mapped_columns=['first_name', 'ssn'])
    q = c.assemble_comparison()
    logger.debug(q)
    assert len(q) > 0
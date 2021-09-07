# test_preprocessing.py

import pytest

from config import BIGQUERY_TEST_TABLE

from preprocess.sql import compose_preprocessing_query, compose_preprocessed_table_query
from preprocess.preprocess import preprocess_table

from gcp.client import get_bigquery_client

import logging 
logger = logging.getLogger(__name__)

def generate_input():
    return {
            'mapping': {
                'first_name': 'FIRST_NAME',
                'last_name': 'LAST_NAME', 
                'ssn': 'SSN', 
                # 'gender':'gender'
                },
            'partner': 'ADHOC',
            'tablename': BIGQUERY_TEST_TABLE
            }

@pytest.fixture
def api_input():
    return generate_input()


@pytest.mark.incremental
class TestPreprocessing:

    def test_compose_query(self, api_input):
        q = compose_preprocessing_query(**api_input)
        fq = compose_preprocessed_table_query(**api_input)
        for element in [q, fq]:
            assert element is not None
            assert len(element) > 0
        logger.debug(fq)


    def test_full_preprocessing_bigquery(self, api_input):
        res = preprocess_table(**api_input)
        assert res is not None

        # Teardown (move to test teardown later)
        client = get_bigquery_client()
        client.delete_table(res, not_found_ok=False)  # table should be found else early error

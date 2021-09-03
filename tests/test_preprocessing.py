# test_preprocessing.py

import pytest

from preprocess.sql import compose_preprocessing_query, compose_preprocessed_table_query
from preprocess.preprocess import preprocess_table

import logging 
logger = logging.getLogger(__name__)

def generate_input():
    return {
            'mapping': {'first_name': 'FIRST_NAME', 'last_name': 'LAST_NAME', 'ssn': 'SSN'},
            'partner': 'ADHOC',
            'tablename': '`ut-dws-udrc-dev.ADHOC.GRADUATES_002D66FA-300A-4018-814F-A68E07D811A1`'
            }

@pytest.fixture
def api_input():
    return generate_input()


def test_compose_query(api_input):
    q = compose_preprocessing_query(**api_input)
    fq = compose_preprocessed_table_query(**api_input)
    for element in [q, fq]:
        assert element is not None
        assert len(element) > 0
    logger.debug(fq)


def test_full_preprocessing_bigquery(api_input):
    res = preprocess_table(**api_input)
    assert res is not None
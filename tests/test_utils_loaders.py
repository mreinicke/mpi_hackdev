# test_utils_loaders.py


import pytest
from mpi.settings import config

from mpi.utils.loaders import load_bigquery_table, create_generator_from_iterators

def generate_input():
    return {
            'mapping': {
                'first_name': 'FIRST_NAME',
                'last_name': 'LAST_NAME', 
                'ssn': 'SSN', 
                # 'gender':'gender'
                },
            'partner': 'ADHOC',
            'tablename': config.BIGQUERY_TEST_PREPROCESSED_TABLE
            }

@pytest.fixture
def api_input():
    return generate_input()


def test_bigquery_table_load(api_input):
    tablename = api_input['tablename']
    assert load_bigquery_table(tablename) is not None


def test_create_generator_from_iterators():
    l1 = [1, 2, 3]
    l2 = tuple([4, 5, 6])
    l3 = range(1, 10, 2)
    seq = create_generator_from_iterators(l1, l2, l3)
    for i, _ in enumerate(seq):
        pass 
    assert i + 1 == len(l1) + len(l2) + 5


def test_sequence_generator_with_bigquery():
    raise NotImplementedError("Write this test to ensure all rows plus messages works as expected")
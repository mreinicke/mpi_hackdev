# test_utils_loaders.py


import pytest
from config import BIGQUERY_TEST_TABLE
from config import BIGQUERY_TEST_PREPROCESSED_TABLE

from utils.loaders import load_bigquery_table

def generate_input():
    return {
            'mapping': {
                'first_name': 'FIRST_NAME',
                'last_name': 'LAST_NAME', 
                'ssn': 'SSN', 
                # 'gender':'gender'
                },
            'partner': 'ADHOC',
            'tablename': BIGQUERY_TEST_PREPROCESSED_TABLE
            }

@pytest.fixture
def api_input():
    return generate_input()


def test_bigquery_table_load(api_input):
    tablename = api_input['tablename']
    assert load_bigquery_table(tablename) is not None
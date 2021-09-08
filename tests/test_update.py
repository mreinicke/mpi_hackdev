# test_update.py

import pytest
from update.update import update_preprocessed_table

from config import BIGQUERY_TEST_PREPROCESSED_TABLE

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


def test_update_preprocessed_table(api_input):
    err, tablename = update_preprocessed_table(api_input['tablename'])

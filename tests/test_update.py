# test_update.py

import pytest
from update.update import update_preprocessed_table, get_rows_from_table

from config import BIGQUERY_TEST_PREPROCESSED_TABLE

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
            'tablename': BIGQUERY_TEST_PREPROCESSED_TABLE
            }

@pytest.fixture
def api_input():
    return generate_input()


# def test_update_preprocessed_table(api_input):
#     err, tablename = update_preprocessed_table(api_input['tablename'])


def test_serialize_biguquery_row(api_input):
    rows = get_rows_from_table(api_input['tablename'])
    logger.debug(rows)
# test_update.py

import pytest
import json

from update.update import (
    update_preprocessed_table, 
    serialize_rows_from_table,
    push_rows_to_firestore,
    update_firestore_from_table,
)
from gcp.models import Context

from config import BIGQUERY_TEST_PREPROCESSED_TABLE, BIGQUERY_LARGE_PREPROCESSED

import logging
logger = logging.getLogger(__name__)


@pytest.fixture
def context():
    return Context(
        raw = json.dumps({'guid': 'testguid_test_update_1'})
    )


def test_update_preprocessed_table(api_input):
    err, tablename = update_preprocessed_table(api_input['tablename'])


def test_serialize_biguquery_row(context):
    rows = serialize_rows_from_table(context, tablename=BIGQUERY_TEST_PREPROCESSED_TABLE)
    assert len(rows) > 0


def test_push_rows_to_firestore(context):
    rows = serialize_rows_from_table(context, tablename=BIGQUERY_TEST_PREPROCESSED_TABLE)
    push_rows_to_firestore(rows)


# def test_threaded_update_handler(context):
#     update_firestore_from_table(context, tablename=BIGQUERY_TEST_PREPROCESSED_TABLE)
    # update_firestore_from_table(context, tablename=BIGQUERY_LARGE_PREPROCESSED)
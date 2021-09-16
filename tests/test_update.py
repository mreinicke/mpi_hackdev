# test_update.py

import pytest
import json

from update import update_preprocessed_table

from update.update import (
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
        raw = json.dumps({'guid': 'testguid_test_update_1', 'sourceTable': BIGQUERY_TEST_PREPROCESSED_TABLE})
    )


def test_update_preprocessed_table(context):
    err, tablename = update_preprocessed_table(context)
    assert tablename == context.source_tablename


def test_serialize_biguquery_row(context):
    rows = serialize_rows_from_table(context, tablename=BIGQUERY_TEST_PREPROCESSED_TABLE)
    assert len(rows) > 0


# def test_push_rows_to_firestore(context):  ## Deprecated.  Use threaded handler.
#     rows = serialize_rows_from_table(context, tablename=BIGQUERY_TEST_PREPROCESSED_TABLE)
#     push_rows_to_firestore(rows)


def test_threaded_update_handler(context):
    update_firestore_from_table(context, tablename=BIGQUERY_TEST_PREPROCESSED_TABLE)  ##TODO: teardown needed for multiple runs
    # update_firestore_from_table(context, tablename=BIGQUERY_LARGE_PREPROCESSED)
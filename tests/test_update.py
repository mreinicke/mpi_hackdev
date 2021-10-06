# test_update.py

import pytest
import json

from ..update import update_preprocessed_table
from ..update.update import (
    serialize_rows_from_table,
    update_firestore_from_table,
    push_rows_to_firestore
)
from ..gcp.client import get_firestore_client
from ..gcp.models import Context
from ..settings import config

import logging
logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def context():
    return Context(
        raw = json.dumps({'guid': 'testguid_test_update_1', 'sourceTable': config.BIGQUERY_TEST_PREPROCESSED_TABLE})
    )


def test_update_preprocessed_table(context):
    err, tablename = update_preprocessed_table(context)
    assert tablename == context.source_tablename


def test_serialize_biguquery_row(context):
    rows = serialize_rows_from_table(context, tablename=config.BIGQUERY_TEST_PREPROCESSED_TABLE)
    assert len(rows) > 0




@pytest.mark.incremental
class TestUpdateFirestoreMethods:

    def test_push_rows_to_firestore(self, context):
        rows = serialize_rows_from_table(context, tablename=config.BIGQUERY_TEST_PREPROCESSED_TABLE)
        mpis = [row['mpi'] for row in rows]
        push_rows_to_firestore(rows)  ## Will pop mpi out of the dict.  cannot access mpi anymore. see above for mpis
        client = get_firestore_client()
        collection = client.collection(config.FIRESTORE_IDENTITY_POOL)
        for mpi in mpis:
            collection.document(mpi).delete()


    def test_threaded_update_handler(self, context):
        update_firestore_from_table(context, tablename=config.BIGQUERY_TEST_PREPROCESSED_TABLE)
        # update_firestore_from_table(context, tablename=BIGQUERY_LARGE_PREPROCESSED)
        raise NotImplementedError("Teardown not implemented for this method.  Cannot re-run test without manual record deletion at present")
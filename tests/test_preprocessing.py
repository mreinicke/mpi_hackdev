# test_preprocessing.py

import pytest

from preprocess.sql import compose_preprocessing_query, compose_preprocessed_table_query
from preprocess.preprocess import preprocess_table

from gcp.client import get_bigquery_client
from gcp.models import Context

from uuid import uuid4
from random import choice
import json

from settings import config

import logging 
logger = logging.getLogger(__name__)



def generate_raw_ui_message() -> str:
    return json.dumps(
        {
            "sourceTable": config.BIGQUERY_TEST_TABLE,
            # "sourceTable": BIGQUERY_LARGE,
            "guid": str(uuid4()),
            'partner': choice(['USHE', 'USBE', 'UDOH', 'ADHOC', 'USTC']),
            "operation":"new",
            "destination":"SPLIT_1_OF_2_LINKED_USBE_HS_COHORT_COMPLETION_SAMPLE",
            "columns":[
                {"name":"STUDENT_ID","outputs":{}},
                {"name":"COHORT_TYPE","outputs":{"DI":{"name":"COHORT_TYPE"}}},
                {"name":"COHORT_YEAR","outputs":{"DI":{"name":"COHORT_YEAR"}}},
                {"name":"DISTRICT_ID","outputs":{"DI":{"name":"DISTRICT_ID"}}},
                {"name":"SCHOOL_ID","outputs":{"DI":{"name":"SCHOOL_ID"}}},
                {"name":"SCHOOL_NBR","outputs":{"DI":{"name":"SCHOOL_NBR"}}},
                {"name":"HS_COMPLETION_STATUS","outputs":{"DI":{"name":"HS_COMPLETION_STATUS"}}},
                {"name":"ENTRY_DATE","outputs":{"DI":{"name":"ENTRY_DATE"}}},
                {"name":"SCHOOL_YEAR","outputs":{"DI":{"name":"SCHOOL_YEAR"}}},
                {"name":"FIRST_NAME","outputs":{"MPI":{"name":"first_name"}}},
                {"name":"LAST_NAME","outputs":{"MPI":{"name":"last_name"}}},
                {"name":"SSN","outputs":{"MPI":{"name":"ssn"}}},
            ]
        }
    )



def generate_context() -> Context:
    return Context(raw=generate_raw_ui_message())


@pytest.fixture
def context():
    return generate_context()


@pytest.mark.incremental
class TestPreprocessing:

    def test_compose_query(self, context):
        q = compose_preprocessing_query(context)
        fq = compose_preprocessed_table_query(context)
        for element in [q, fq]:
            assert element is not None
            assert len(element) > 0
        logger.debug(fq)


    def test_full_preprocessing_bigquery(self, context):
        res = preprocess_table(context)
        assert res is not None

        # Teardown (move to test teardown later)
        client = get_bigquery_client()
        client.delete_table(res, not_found_ok=False)  # table should be found else early error

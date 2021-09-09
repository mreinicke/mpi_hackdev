# test_models.py


import pytest

from uuid import uuid4
from random import choice
import json

from gcp.models import (
    MPIRecord,
    filter_dict_for_allowed_pii,
    build_source_record_from_row,
    build_mpi_record_from_row,
    NoSQLSerializer,
    Context
    )
from config import ALLOWED_PII, BIGQUERY_TEST_TABLE, BIGQUERY_TEST_PREPROCESSED_TABLE

import logging
logger = logging.getLogger(__name__)


def generate_raw_ui_message() -> str:
    return json.dumps(
        {
            "sourceTable": BIGQUERY_TEST_PREPROCESSED_TABLE,
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


@pytest.fixture
def raw_ui_message():
    return generate_raw_ui_message()


def generate_context() -> Context:
    return Context(raw=generate_raw_ui_message())


def generate_row() -> list:
    good_source = {
        'mpi': str(uuid4()),
        'ssn': 123456789,
        'first_name': 'shane',
        'last_name': 'laury',
        'middle_name': 'august',
        'ssid': 'adfa4e684a68f4e6ad4f6a8d',
        'ushe_student_id': '46a4df68a4e6d',
        'usbe_student_id': '45d6a4f1d6a8e46afd4fa6g45da86e2',
        'gender': 'f',
        'birth_date': '1999-09-19',
        'ethnicity': 'h',
        'prob_match': 1.0,
    }
    return good_source


@pytest.fixture
def example_data():
    rows = [generate_row() for _ in range(100)]
    context = generate_context()
    return rows, context


def test_row_key_filter(example_data):
    row = example_data[0][0]
    filtered = filter_dict_for_allowed_pii(row)
    for k in ALLOWED_PII:
        assert k in filtered.keys()
        assert row[k] == filtered[k]


def test_source_record_marshal(example_data):
    rows, context = example_data
    r = build_source_record_from_row(row=rows[0], context=context)
    assert r is not None
    assert r.guid == context.guid


def test_mpi_record_marshal(example_data):
    rows, context = example_data
    mpi_record = build_mpi_record_from_row(row=rows[0], context=context)
    assert mpi_record.mpi == rows[0]['mpi']


def test_serializer(example_data):
    rows, context = example_data
    serializer = NoSQLSerializer(context)
    mpi_records = [serializer(row) for row in rows]
    assert len(mpi_records) == len(rows)
    for i, rec in enumerate(mpi_records):
        assert rec.mpi == rows[i]['mpi']


def test_raw_ui_message_is_str(raw_ui_message):
    assert type(raw_ui_message) == str


def test_mpi_model_as_dict(example_data):
    rows, context = example_data
    mpi_records = [build_mpi_record_from_row(row=row, context=context) for row in rows]
    assert len(mpi_records) == len(rows)
    assert type(mpi_records[0]) == MPIRecord
    assert type(mpi_records[0].as_dict()) == dict
    logger.debug(f"MPI Record as dict: \n{mpi_records[0].as_dict()}")

# test_models.py


import pytest

from uuid import uuid4
from random import choice

from gcp.models import (
    SourceRecord, MPIRecord, 
    filter_dict_for_allowed_pii,
    build_source_record_from_row,
    build_mpi_record_from_row,
    )
from config import ALLOWED_PII


def generate_context() -> dict:
    return {
        'guid': str(uuid4()),
        'partner': choice(['USHE', 'USBE', 'UDOH', 'ADHOC', 'USTC'])
    }


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
    rows = [generate_row() for _ in range(10)]
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
    assert r.guid == context['guid']


def test_mpi_record_marshal(example_data):
    rows, context = example_data
    mpi_record = build_mpi_record_from_row(row=rows[0], context=context)
    assert mpi_record.mpi == rows[0]['mpi']


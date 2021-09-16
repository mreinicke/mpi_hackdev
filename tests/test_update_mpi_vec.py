# test_update_mpi_vec.py

import pytest

from gcp.models import Context
from gcp.client import get_firestore_client
from config import FIRESTORE_IDENTITY_POOL

from update.firestore_to_bigquery.local_utils import create_context_from_string, MPIVectorizer
from update.firestore_to_bigquery.pipeline import run_pipeline
import argparse
import json 

import logging 
logger = logging.getLogger(__name__)


@pytest.fixture
def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', dest='r', type=create_context_from_string, required=True)
    return parser
    

@pytest.fixture
def valid_mpis():
    client = get_firestore_client()
    col = client.collection(FIRESTORE_IDENTITY_POOL)
    docs = col.where(u'updated', u'>', 0).limit(5)
    return [d.id for d in docs.get()]


# def test_parser_raw(parser):
#     parsed = parser.parse_args(['-r', json.dumps({'sourceTable': 'sometablename'})])
#     assert type(parsed.r) == Context
#     assert parsed.r.source_tablename == 'sometablename'


# def test_mpi_vectorizer(valid_mpis):
#     assert len(valid_mpis) > 0, 'No MPIs returned from fixture.'
#     vect = MPIVectorizer()
#     for mpi in valid_mpis:
#         logger.debug(vect.process(mpi))


def test_pipeline():
    run_pipeline()
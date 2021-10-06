# test_update_mpi_vec.py
import pytest

from ..gcp.models import Context
from ..gcp.client import get_firestore_client
from ..utils.runners import send_query
from ..utils.pipeline_utils.udrc_parser import create_context_from_string
from ..update.pipeline.local_utils import MPIVectorizer
from ..pipeline_update_firestore_to_bigquery import run_pipeline

import argparse
import json 

from ..settings import config

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
    col = client.collection(config.FIRESTORE_IDENTITY_POOL)
    docs = col.where(u'updated', u'>', 0).limit(5)
    return [d.id for d in docs.get()]


def test_parser_raw(parser):
    parsed = parser.parse_args(['-r', json.dumps({'sourceTable': 'sometablename'})])
    assert type(parsed.r) == Context
    assert parsed.r.source_tablename == 'sometablename'


def test_mpi_vectorizer(valid_mpis):
    assert len(valid_mpis) > 0, 'No MPIs returned from fixture.'
    vect = MPIVectorizer()
    for mpi in valid_mpis:
        assert vect.process(mpi) is not None



# def test_pipeline():
#     run_pipeline()
#     # Delete all mpi vectors in table where mpi in mpi_list
#     mpi_vector_delete_query = f"DELETE FROM `{config.MPI_VECTORS_TABLE}` WHERE 1=1"
#     err, _ = send_query(mpi_vector_delete_query, verbose=True)
#     if err is not None:
#         raise err
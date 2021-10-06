"""Preprocess

    Convert source table into pre-processed table for indexing.
"""
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from mpi.preprocess.sql import compose_preprocessed_table_query, compose_delete_table_if_exists
from mpi.gcp.models import Context
from mpi.gcp.client import get_bigquery_client
from mpi.utils.runners import logger_wrap, send_query

from typing import Tuple

from logging import getLogger

logger = getLogger(__name__)


@logger_wrap
def preprocess_table(context: Context, client: bigquery.Client = None):

    query, tablename = compose_preprocessed_table_query(context)
    delquery = compose_delete_table_if_exists(tablename=tablename)

    logger.info(f'Sending delete query: {delquery}')
    err, _ = send_query(query=delquery, verbose=True, client=client)
    if err is not None:
        raise err

    logger.info(f'Sending query: {query}')
    err, _ = send_query(query=query, verbose=True, client=client)
    if err is not None:
        raise err
    
    err, tablename = verify_table_created(client=client, tablename=tablename)
    if err is not None:
        raise err

    return tablename
    

def verify_table_created(client, tablename) -> Tuple[BaseException, str]:
    if client is None:
        client = get_bigquery_client()

    try:
        client.get_table(tablename)
        logger.info('Table creation successful')
        return None, tablename
    except NotFound:
        logger.error(f'Table {tablename} not created.')
        return NotFound, tablename
    
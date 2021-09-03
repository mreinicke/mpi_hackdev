"""Preprocess

    Convert source table into pre-processed table for indexing.
"""
from google.cloud.exceptions import NotFound

from preprocess.sql import compose_preprocessed_table_query
from gcp.client import get_bigquery_client

from utils.runners import logger_wrap
from logging import exception, getLogger

import time

logger = getLogger(__name__)


@logger_wrap
def preprocess_table(*args, **kwargs):
    client = get_bigquery_client()
    query, tablename = compose_preprocessed_table_query(*args, **kwargs)

    logger.info(f'Sending query: {query}')
    query_job = client.query(query)

    logger.info(f'Created query_job {query_job.job_id}')
    while not query_job.done():  ## TODO: this can move await and the whole function can go async
        time.sleep(1)

    try:
        client.get_table(tablename)
        logger.info('Table creation successful')
    except NotFound:
        logger.error('Table {tablename} not created.')
        raise ValueError('Preprocessing failed.  Table creation query did not complete.')
    except exception as e:
        logger.error(e)
        raise ValueError(e)

    return tablename

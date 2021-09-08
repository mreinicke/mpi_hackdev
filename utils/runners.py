# runners.py

import time
from gcp.client import get_bigquery_client

import asyncio 
from functools import wraps, partial

import config
import logging

# An async wrapper to add functions to event loop
def async_wrap(func):
    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)
    
    return run


# An argument logger wrapper to log a message and arguments passed to function
def logger_wrap(func):
    @wraps(func)
    def run(*args, **kwargs):
        logger = logging.getLogger(func.__name__)
        logger.info(f'{args}')
        logger.info(f'{kwargs}')
        return func(*args, **kwargs)
    
    return run


# Run BigqueryQuery - Blocks until complete
def send_query(query: str) -> tuple:
    err = None
    client = get_bigquery_client()

    llog = logging.getLogger(__name__)
    llog.info(f'Sending query: {query}')

    query_job = client.query(query)
    llog.info(f'Created query_job {query_job.job_id}')
    try:
        res = query_job.result()
        return err, res
    except Exception as e:
        return e, None
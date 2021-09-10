# runners.py

import time
from gcp.client import get_bigquery_client

import asyncio 
from functools import wraps, partial

import queue
from concurrent.futures import ThreadPoolExecutor

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


# MultiThreading Handler

class QueueJobHander():
    """QueueJobHandler
        Maintain logic to put items into and get items out of 
        a thread-safe queue.  Wraps push function (infn) and pop function (outfn)
        as futures.  Instantiates a threadpool executor.  Checks completeness.

        V0.1 Built assuming only in_fn needs to work on sequence
    """
    def __init__(self, infn, outfn, sequence=None, num_threads=2, queue_max_size=100):
        self.infn = infn
        self.outfn = outfn
        self.queue = queue.Queue(queue_max_size)
        self.sequence = sequence
        self.num_threads = num_threads

    def run(self):
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            infn = partial(self.infn, sequence=self.sequence, queue=self.queue)
            outfn = partial(self.outfn, queue=self.queue)
            out_future = executor.submit(outfn)
            in_future = executor.submit(infn)

            return in_future.result(), out_future.result()




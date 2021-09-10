#test_utils.py

import asyncio
from utils.runners import (
    async_wrap, 
    logger_wrap, 
    send_query, 
    QueueJobHander,
    BatchFilter,
)
from utils.embeds import AlphabetVectorizer

import logging

logger = logging.getLogger(__name__)

@async_wrap
def long_function():
    assert 1==1


@logger_wrap
def logger_wrapped_fn(*args, **kwargs):
    assert 1==1


def test_long_function():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(
        long_function()))
    loop.close()


def test_logger_wrapped_function():
    logger_wrapped_fn(215, hello='world')


def test_send_query():
    query = "SELECT 1 FROM DUAL;"
    res, err = send_query(query)
    assert err is None
    assert res is not None  # Change to ensure (1) is returning as a row


def test_alphabet_vectorizer():
    clf = AlphabetVectorizer()
    assert len(clf('test_str')) == 26


def test_queue_job_handler_basic_io():
    from queue import Queue
    def _infn(sequence, queue: Queue, **kwargs):
        for message in sequence:
            queue.put(message)
        return 'complete'
        
    def _outfn(*args, queue: Queue = None, **kwargs):
        while True:
            if queue.not_empty:
                m = queue.get()
                logger.debug(f'queue_handler: {m}')
                if m == 'done':
                    queue.task_done()
                    break
                queue.task_done()
            
    messages = ['I am a message', 'done']
    handler = QueueJobHander(infn=_infn, outfn=_outfn, sequence=messages)
    assert handler is not None
    logger.debug('Starting queue handler')
    handler.run()


def test_batch_filter():
    pass
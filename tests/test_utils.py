#test_utils.py

import asyncio
from utils.runners import async_wrap, logger_wrap, send_query
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
    assert res is not None


def test_alphabet_vectorizer():
    clf = AlphabetVectorizer()
    assert len(clf('test_str')) == 26
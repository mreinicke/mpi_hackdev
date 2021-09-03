# content of conftest.py

from typing import Dict, Tuple
import pytest

from config import logfile
from time import strftime,gmtime
 
# store history of failures per test class name and per index in parametrize (if parametrize used)
_test_failed_incremental: Dict[str, Dict[Tuple[int, ...], str]] = {}


def pytest_runtest_makereport(item, call):
    if "incremental" in item.keywords:
        # incremental marker is used
        if call.excinfo is not None:
            # the test has failed
            # retrieve the class name of the test
            cls_name = str(item.cls)
            # retrieve the index of the test (if parametrize is used in combination with incremental)
            parametrize_index = (
                tuple(item.callspec.indices.values())
                if hasattr(item, "callspec")
                else ()
            )
            # retrieve the name of the test function
            test_name = item.originalname or item.name
            # store in _test_failed_incremental the original name of the failed test
            _test_failed_incremental.setdefault(cls_name, {}).setdefault(
                parametrize_index, test_name
            )


def pytest_runtest_setup(item):
    if "incremental" in item.keywords:
        # retrieve the class name of the test
        cls_name = str(item.cls)
        # check if a previous test has failed for this class
        if cls_name in _test_failed_incremental:
            # retrieve the index of the test (if parametrize is used in combination with incremental)
            parametrize_index = (
                tuple(item.callspec.indices.values())
                if hasattr(item, "callspec")
                else ()
            )
            # retrieve the name of the first test function to fail for this class name and index
            test_name = _test_failed_incremental[cls_name].get(parametrize_index, None)
            # if name found, test has failed for the combination of class name & test name
            if test_name is not None:
                pytest.xfail("previous test failed ({})".format(test_name))


class FileLogger():
    def __init__(self, logfile):
        self.logfile = logfile
        self._write_to_file(f'Log Start', name=__name__)


    def _write_to_file(self, message, name):
        with open(logfile, 'a+') as f:
            timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
            f.writelines(f'{timestamp} {name} {message} \n')

    def info(self, message, name='und'):
        self._write_to_file(f'INFO: {message}', name)

    def debug(self, message, name='und'):
        self._write_to_file(f'DEBUG: {message}', name)

    def error(self, message, name='und'):
        self._write_to_file(f'ERROR: {message}', name)



testlogger = FileLogger(logfile=logfile)
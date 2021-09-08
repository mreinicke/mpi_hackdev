# config.py

from decouple import config

import logging


DEBUG = config('DEBUG', default=True, cast=bool)
logfile = config('LOGFILE', default=None)

if DEBUG:
    loglevel = 'DEBUG'
else:
    loglevel = 'INFO'


def log_setup(loglevel, logfile):
    if loglevel == 'DEBUG':
        level = logging.DEBUG
    else:
        level = logging.INFO
    
    logging.basicConfig(
        format='[%(asctime)s] %(levelname)s | %(name)s: %(message)s',
        filename=logfile, 
        level=level)

log_setup(loglevel, logfile)
            
BQ_PROJECT = 'ut-dws-udrc-dev'
BQ_DATASET = 'ADHOC'

FIRESTORE_IDENTITY_POOL = config('FIRESTORE_IDENTITY_POOL', default=None)

BIGQUERY_TEST_TABLE = config('BIGQUERY_TEST_TABLE', default=None)

BIGQUERY_TEST_PREPROCESSED_TABLE = config('BIGQUERY_TEST_PREPROCESSED_TABLE', default=None)

GCS_BUCKET_NAME = config('GCS_BUCKET_NAME', default=None)

ALLOWED_PII = config('ALLOWED_PII', cast=lambda v: [s.strip() for s in v.split(',')], default=None) 
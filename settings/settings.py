# settings.py

import logging
from pydantic import BaseSettings
from typing import List


class Settings(BaseSettings):
    DEBUG: bool
    LOGFILE: str
    ALLOWED_PII: List[str]

    BIGQUERY_TEST_TABLE: str
    BIGQUERY_TEST_PREPROCESSED_TABLE: str
    BIGQUERY_LARGE: str
    BIGQUERY_LARGE_PREPROCESSED: str

    GCP_PROJECT_ID: str
    MPI_SERVICE_SECRET_NAME: str

    MPI_VECTORS_TABLE: str
    FIRESTORE_IDENTITY_POOL: str
    GCS_BUCKET_NAME: str

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'



config = Settings()

if config.DEBUG:
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

log_setup(loglevel, config.LOGFILE)
logger = logging.getLogger(__name__)
logger.info(f"Configured logging loglevel {loglevel}")
# settings.py

import logging
from pydantic import BaseSettings
from typing import List, Optional


class Settings(BaseSettings):
    DEBUG: bool = True
    LOGLEVEL: str = 'DEBUG'
    LOGFILE: str = 'systemdat.log'
    ALLOWED_PII: List[str] = ["first_name","last_name","ssn","ssid","middle_name","ushe_student_id","usbe_student_id","ustc_student_id","gender","ethnicity","birth_date"]

    BIGQUERY_TEST_TABLE: str = 'ut-dws-udrc-dev.ADHOC.GRADUATES_002D66FA-300A-4018-814F-A68E07D811A1'
    BIGQUERY_TEST_PREPROCESSED_TABLE: str = 'ut-dws-udrc-dev.ADHOC.GRADUATES_002D66FA-300A-4018-814F-A68E07D811A1_preprocessed'

    GCP_PROJECT_ID: Optional[str]
    MPI_SERVICE_SECRET_NAME: Optional[str]

    MPI_VECTORS_TABLE: Optional[str ]
    FIRESTORE_IDENTITY_POOL: Optional[str ]
    GCS_BUCKET_NAME: Optional[str]

    INDEX_NAME_NEIGHBOR_THRESHOLD: Optional[float] = 0.2
    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'



config = Settings()

def log_setup(loglevel, logfile):
    if loglevel == 'DEBUG':
        level = logging.DEBUG
    else:
        level = logging.INFO
    
    logging.basicConfig(
        format='[%(asctime)s] %(levelname)s | %(name)s: %(message)s',
        filename=logfile, 
        level=level)

log_setup(config.LOGLEVEL, config.LOGFILE)
logger = logging.getLogger(__name__)
logger.info(f"Configured logging loglevel {config.LOGLEVEL}")
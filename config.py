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

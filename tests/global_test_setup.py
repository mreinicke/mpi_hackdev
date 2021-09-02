from config import logfile
from time import strftime,gmtime
 
 
 



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
# preprocess.pipeline.local_utils.py

import apache_beam as beam

from mpi.gcp.client import get_bigquery_client
from mpi.gcp.models import Context
from mpi.preprocess.preprocess import preprocess_table
from mpi.settings import Settings

import logging

config = Settings()
logger = logging.getLogger(__name__)


class PreprocessTableFn(beam.DoFn):

    def __init__(   
        self, 
        context: Context,
        secret: str = config.MPI_SERVICE_SECRET_NAME,
        ) -> None:

        self.secret = secret
        self.context = context
        super().__init__()


    def process(self, *args):
        self.bigquery_client = get_bigquery_client(secret=self.secret)
        res = preprocess_table(context=self.context, client=self.bigquery_client)
        assert res is not None, res
        self.bigquery_client.close()

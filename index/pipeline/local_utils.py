#index pipeline local utils

import apache_beam as beam
from collections import namedtuple
from gcp.client import get_bigquery_client
from index.index import NameIndexer
from utils.runners import send_query
from settings import Settings
from typing import List, Tuple
import logging

config = Settings()
logger = logging.getLogger(__name__)


def delete_table_if_exists(tablename: str):
	query = f"DROP TABLE `{tablename}_index`"
	err, _ = send_query(query=query, verbose=True)
	return err


NameElement = namedtuple('NameElement', ['first_name', 'last_name', 'rownum'])

class NameMatchIndexFn(beam.DoFn):

	def __init__(
		self, 
		secret: str = config.MPI_SERVICE_SECRET_NAME,
		max_batch_size: int = 100,
		mapped_columns: List[str] = None,
		tablename: str = config.BIGQUERY_TEST_PREPROCESSED_TABLE,
		bucket: str = config.GCS_BUCKET_NAME
		) -> None:

		self.batch = []
		self.secret = secret
		self.max_batch_size = max_batch_size
		assert mapped_columns is not None, 'Must provide list of mapped columns'
		self.mapped_columns = mapped_columns
		self.tablename = tablename
		self.bucket = bucket
		super().__init__()


	def _flush(self):
		print(self.batch)
		self.batch = []


	def start_bundle(self):
		self.bigquery_client = get_bigquery_client(secret=self.secret)
		self.indexer = NameIndexer(
			mapped_columns=self.mapped_columns,
			preprocessed_table=self.tablename,
			secret=self.secret,
			bucket=self.bucket,
		)
		self._flush()


	def process(self, element: Tuple[str, str, int]):
		if len(self.batch) >= self.max_batch_size:
			self._flush()
		self.batch.append(NameElement._make(element))


	def finish_bundle(self):
		self._flush()
		self.bigquery_client.close()
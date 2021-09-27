"""firestore_to_bigquery.local_utils
"""

from typing import Set
from utils.runners import send_query
from gcp.client import get_bigquery_client, get_firestore_client
from gcp.models import Context, MPIVector

import apache_beam as beam
from google.cloud import firestore

import argparse
import math
from itertools import product
import pandas as pd
from utils.iterators import coalesce

from settings import Settings

config = Settings()

import logging
logger = logging.getLogger(__name__)

# Control Classes - Handle argument parsing and pipeline setup
class CustomArgParserFactory():
	def __init__(self, **kwargs):
		self.kwargs = kwargs
		self.parser = argparse.ArgumentParser()
	
	def __call__(self) -> argparse.ArgumentParser:
		self.__add_argparse_args()
		return self.parser

	def __add_argparse_args(self):
		self.parser.add_argument(
			'--context',
			type=create_context_from_string,
			help='Raw UI process instructions',
		)
		self.parser.add_argument(
			'--project',
			type=str,
			help='project name (not gcp assigned id)',
		)
		self.parser.add_argument(
			'--debug',
			type=bool,
			default=True,
		)
		self.parser.add_argument(
			'--secret',
			type=str,
			help='name of secret for service acount credential creation'
		)
		self.parser.add_argument(
			'--vectable',
			type=str,
			help='fully qualified mpi vectors table name: project.dataset.tablename'
		)
		self.parser.add_argument(
			'--collection',
			type=str,
			help='firestore identity pool collection name'
		)
		self.parser.add_argument(
			'--bucket',
			type=str,
			help='name of bucket to store process assets and dataflow stuff'
		)



# Parse raw data from UI (stored in CloudSQL) into Context object
def create_context_from_string(m: str) -> Context:
	return Context(raw=m)



# Log options when pipeline is created
class LogPipelineOptionsFn(beam.DoFn):
	def __init__(self, options, message: str = None, options_type: str = None):
		self.options = options
		self.message = message
		self.options_type = options_type
		assert self.options_type in ['pipeline', 'other'], \
			'must declare options_type=pipeline or other'

	def process(self, *args, **kwargs):
		logger.info(self.message)
		if self.options_type == 'pipeline':
			try:
				logger.info('PipelineOptions Handler yields:  %s' % self.options.get_all_options())
			except Exception as e:
				logger.error(f'could not display pipeline options: {e}')
		elif self.options_type == 'other':
			try:
				logger.info(f'{self.options}')
			except Exception as e:
				logger.errot(f'could not display other arguments {e}')



# Generate a query to get distince MPIs from a table - used to limit updates to mpi_vectors table
def create_select_mpi_query_from_context(args , tablename = None) -> str:
	assert args.context is not None or tablename is not None, 'Must provide either context object or tablename'
	try:
		tablename = args.context.source_tablename + '_preprocessed'
	except Exception as e:
		logger.warn(e)

	return f"""
	SELECT
		DISTINCT mpi AS mpi
	FROM
		`{tablename}`;
	"""


# Generate a query to delete any MPI vectors with given List of MPIs
def create_delete_mpis_from_mpi_list(args, tablename: str = None) -> str:
	assert args.context is not None or tablename is not None, 'Must provide either context object or tablename'
	try:
		tablename = args.context.source_tablename + '_preprocessed'
	except Exception as e:
		logger.warn(e)
	
	if args.vectable is None:
		vectable = Settings().MPI_VECTORS_TABLE
	else:
		vectable = args.vectable

	return f"""
	DELETE FROM `{vectable}`
	WHERE mpi IN (SELECT DISTINCT mpi FROM `{tablename}`);
	"""




############################
### MPI Vector Generator ###
############################
# Generate a set of MPI vectors from a document record

## Frequency Functions

def mean(proportions):
	return sum(proportions) / len(proportions)

def geomean(proportions):
	return math.prod(proportions) ** (1/len(proportions))


## Vectorizer

def create_mpi_vectors_from_firestore_document(mdoc:firestore.DocumentSnapshot, freqfn=geomean) -> list:

	def _extract_fields(mdoc:dict) -> tuple:
		# Convert array of maps to tuple of tuples where inner tuples are (key, value)
		fields = []
		for s in mdoc['sources']:
			fields.extend(list(zip(s['fields'].keys(), s['fields'].values())))
		return tuple(fields)


	def _index_and_count_values(values:tuple)->dict:
		index = {}
		counts = {}
		for i, v in enumerate(values):
			if v[0] in index:
				index[v[0]].append(i)
			else:
				index[v[0]] = [i]
			if v[1] in counts:
				counts[v[1]] += 1
			else:
				counts[v[1]] = 1
		return index, counts


	def _build_vectors(values: tuple, index: dict, counts:dict, freqfn) -> list:

		def _consolidate_tuple_to_dict(selection: tuple, columns: tuple, values: tuple) -> dict:
			res = {}
			[res.update({x[0]:values[x[1]][1]}) for x in list(zip(columns, selection))]
			return res

		def _calc_freq_score(vect:dict, counts:dict, index:dict, freqfn) -> float:
			proportions = []
			for key in vect:
				proportions.append(
					counts[vect[key]] / len(index[key])
				)
			return freqfn(proportions)

		def _filter_unique(vectors: list):
			return pd.DataFrame(vectors).drop_duplicates().to_dict(orient='r')

		# Generate list of possible vectors as dictionaries
		columns = tuple(index.keys())
		id_sets = [index[k] for k in index.keys()]
		prd = product(*id_sets)
		vectors = [_consolidate_tuple_to_dict(sel, columns, values) for sel in prd if sel is not None]

		# Test vector fidelity by importing into a table and return distinct records
		vectors = _filter_unique(vectors)
		[v.update({'frequency_score': _calc_freq_score(v, counts, index, freqfn)}) for v in vectors]
		return vectors

	def _add_mpi_to_vects(vectors:list, mpi) -> list:
		for v in vectors:
			v.update({'mpi': mpi})
		return vectors

	mpi = mdoc.id
	values = _extract_fields(mdoc.to_dict())
	index, counts = _index_and_count_values(values)
	mvects = _build_vectors(values=values, index=index, counts=counts, freqfn=freqfn)
	mvects = _add_mpi_to_vects(mvects, mpi)
	return mvects



class MPIVectorizer(beam.DoFn):

	def __init__(self, mpi_collection: str = config.FIRESTORE_IDENTITY_POOL, secret: str = config.MPI_SERVICE_SECRET_NAME, freqfn = geomean) -> None:
		self.freqfn = freqfn
		self.vectfn = create_mpi_vectors_from_firestore_document
		self.secret = secret
		self.mpi_collection = mpi_collection
		super().__init__()

	def __call__(self, doc: firestore.DocumentSnapshot):
		return [MPIVector(**vect) for vect in self.vectfn(doc, self.freqfn)]

	def start_bundle(self):
		self.firestore_client = get_firestore_client(secret=self.secret)
		self.firestore_collection = self.firestore_client.collection(self.mpi_collection)

	def process(self, element: str):
		if hasattr(self, 'firestore_collection'):
			firestore_collection = self.firestore_collection
		else:
			firestore_collection = get_firestore_client(secret=self.secret).collection(self.mpi_collection)
		doc = firestore_collection.document(element).get()
		return self(doc)

	def finish_bundle(self):
		self.firestore_client.close()



class MPIVectorTableUpdate(beam.DoFn):
	
	def __init__(self, secret: str = config.MPI_SERVICE_SECRET_NAME, mpi_vectors_table: str = config.MPI_VECTORS_TABLE) -> None:
		super().__init__()
		self.secret = secret
		self.mpi_vectors_table = mpi_vectors_table

	def start_bundle(self):
		self.bigquery_client = get_bigquery_client(secret=self.secret)

	def send_query(self, query: str, verbose=False) -> tuple:
		return send_query(query, verbose, client=self.bigquery_client, no_results=True)

	def process(self, element: MPIVector):
		insert_query = element.as_sql(tablename=self.mpi_vectors_table)
		err, _ = self.send_query(insert_query, verbose = True)

	def finish_bundle(self):
		self.bigquery_client.close()
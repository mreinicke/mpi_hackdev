"""
firestore_to_bigquery.utils
"""
from gcp.models import Context
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import RuntimeValueProvider
from google.cloud import firestore
import apache_beam as beam
import argparse
import math
from itertools import product
import pandas as pd

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
				default='')




# Parse raw data from UI (stored in CloudSQL) into Context object
def create_context_from_string(m: str) -> Context:
	return Context(raw=m)



# Log options when pipeline is created
class LogPipelineOptionsFn(beam.DoFn):
	def __init__(self, options_handle, message: str = None):
		self.options_handle = options_handle
		self.message = message

	def process(self, *args, **kwargs):
		logger.info(self.message)
		try:
			logger.info('PipelineOptions Handler yields:  %s' % self.options_handle.get_all_options())
		except:
			logger.info(f'Other Arugments: {self.options_handle}')



# Generate a query to get distince MPIs from a table - used to limit updates to mpi_vectors table
def create_select_mpi_query_from_context(beam_options: Context = None , tablename = None) -> str:
	assert beam_options is not None or tablename is not None, 'Must provide either context object or tablename'
	try:
		tablename = beam_options.context.source_tablename + '_preprocessed'
	except Exception as e:
		logger.warn(e)

	return f"""
	SELECT
		DISTINCT mpi AS mpi
	FROM
		`{tablename}`;
	"""

# Generate a query to delete any MPI vectors with given MPI
def create_delete_mpi_vector_records_from_context_mpi(mpi: str, beam_options: Context = None, tablename = None) -> str:
	assert beam_options is not None or tablename is not None, 'Must provide either context object or tablename'
	try:
		tablename = beam_options.context.source_tablename + '_preprocessed'
	except Exception as e:
		logger.warn(e)

	return f"""
	DELETE FROM `{tablename}`
	WHERE mpi = {mpi};
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
		[v.update({'freq_score': _calc_freq_score(v, counts, index, freqfn)}) for v in vectors]
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
	logger.debug(mvects)
	# return mvects


class MPIVectorizer():
	def __init__(self, freqfn = geomean) -> None:
		self.freqfn = freqfn
		self.vectfn = create_mpi_vectors_from_firestore_document

	def __call__(self, doc: firestore.DocumentSnapshot):
		return self.vectfn(doc, self.freqfn)
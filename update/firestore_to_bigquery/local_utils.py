"""
firestore_to_bigquery.utils
"""
from gcp.models import Context
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import RuntimeValueProvider
import apache_beam as beam
import argparse
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
			`{tablename}`
	"""
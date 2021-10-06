"""Pipeline Loggers

DoFns to log or export runtime metadata during pipeline execution.
"""

import apache_beam as beam

import logging

logger = logging.getLogger(__name__)

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
				logger.error(f'could not display other arguments {e}')
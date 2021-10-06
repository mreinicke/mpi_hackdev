"""UDRC Common Parser for Dataflow Pipelines

Parser with sensible defaults.  Pipelines may not use all arguments.
"""

import argparse

from ...gcp.models import Context

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
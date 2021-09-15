"""
firestore_to_bigquery pipeline

An apache beam pipeline to update MPI vectors table from firestore
given context
"""

from update.prepare import delete_mpi_vectors_in_table
import apache_beam as beam
from .local_utils import (
    PipelineOptions,
    CustomArgParserFactory,
    LogPipelineOptionsFn,
    create_select_mpi_query_from_context
)

from utils.runners import send_query

import config

import logging

logger = logging.getLogger(__name__)


# Setup Pipeline Options
parser_factory = CustomArgParserFactory()
parser = parser_factory()
args, beam_args = parser.parse_known_args()

beam_options = PipelineOptions(
    beam_args,
    project=config.GCP_PROJECT_ID,
    temp_location=config.GCS_BUCKET_NAME,
    staging_location=config.GCS_BUCKET_NAME,
    service_account_email='udrc-mpi-sa@ut-dws-udrc-dev.iam.gserviceaccount.com',
)

# Create Pipeline
def run_pipeline():
    # Prepare pipeline assets.
    if config.DEBUG:
        tablename = config.BIGQUERY_TEST_PREPROCESSED_TABLE
    else:
        tablename = None

    # Get list of MPIs to work with.  Only MPIs under consideration need to 
    #   have MPI vectors updated
    mpi_query = create_select_mpi_query_from_context(args, tablename=tablename)
    err, res = send_query(mpi_query, verbose=True)
    if err is not None:
        raise err

    mpi_list = [r.mpi for r in res]

    with beam.Pipeline(options=beam_options) as pipeline:
        # Add a branch for logging the ValueProvider value.
        _ = (
            pipeline
            | beam.Create([None])
            | 'LogBeamArgs' >> beam.ParDo(LogPipelineOptionsFn(beam_options, message='Beam Arguments'))
            | 'LogOtherArgs' >> beam.ParDo(LogPipelineOptionsFn(args, message='Other Arguments'))
        )

        mpi_vectors = (
            pipeline
            | 'CreateMappedMPIPCollection' >> beam.Create(mpi_list)
            | 'DebugPrint' >> beam.Map(print)
        )

    pipeline.run()
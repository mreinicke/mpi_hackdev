"""
firestore_to_bigquery pipeline

An apache beam pipeline to update MPI vectors table from firestore
given context.
"""

from update.prepare import delete_mpi_vectors_in_table
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from update.firestore_to_bigquery.local_utils import (
    CustomArgParserFactory,
    LogPipelineOptionsFn,
    create_select_mpi_query_from_context,
    create_delete_mpis_from_mpi_list,
    MPIVectorizer,
    MPIVectorTableUpdate
)

from utils.runners import send_query

from settings import config

import logging

logger = logging.getLogger(__name__)

GCS_BUCKET_FULL_PATH = 'gs://' + config.GCS_BUCKET_NAME + '/index'


# Create Pipeline
def run_pipeline(save_main_session=True):
    # Setup Pipeline Options & Command Line Arguments
    parser_factory = CustomArgParserFactory()
    parser = parser_factory()
    args, beam_args = parser.parse_known_args()

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    beam_options = PipelineOptions(
        beam_args,
        project=config.GCP_PROJECT_ID,
        temp_location=GCS_BUCKET_FULL_PATH,
        staging_location=GCS_BUCKET_FULL_PATH,
        service_account_email='udrc-mpi-sa@ut-dws-udrc-dev.iam.gserviceaccount.com',
    )
    beam_options.view_as(SetupOptions).save_main_session = save_main_session

    # Prepare pipeline assets. Check against command line arguments and environment files
    # Datafalow runners may not have access to an up-to-date environment.  Override with
    # arguments if provided.
    if config.DEBUG:
        tablename = config.BIGQUERY_TEST_PREPROCESSED_TABLE
    else:
        tablename = None
        raise NotImplementedError('Context tablename not implemented for this pipeline')

    if args.collection is not None:
        mpi_collection = args.collection
    else:
        mpi_collection = config.FIRESTORE_IDENTITY_POOL

    if args.secret is not None:
        secret = args.secret
    else:
        secret = config.MPI_SERVICE_SECRET_NAME

    # Get list of MPIs to work with.  Only MPIs under consideration need to 
    # have MPI vectors updated
    # TODO: integrate into pipeline for better row handling.  Either fix permissions issues or
    # re-implement streaming from table, chunking, bundling, etc. with given client.  This implementation
    # cannot handle large tables as it will attempt to put a list of all affected MPIs into memory 
    # (ok for a couple thousand, not a couple million)
    mpi_query = create_select_mpi_query_from_context(args, tablename=tablename)
    err, res = send_query(mpi_query, verbose=True)
    if err is not None:
        raise err

    mpi_list = [r.mpi for r in res]

    # Delete all mpi vectors in table where mpi in mpi_list
    mpi_vector_delete_query = create_delete_mpis_from_mpi_list(args, tablename=tablename)
    err, res = send_query(mpi_vector_delete_query, verbose=True)
    if err is not None:
        raise err

    logger.warn('PIPELINE DEFINED')
    with beam.Pipeline(options=beam_options) as pipeline:
        # Add a branch for logging the ValueProvider value.
        _ = (
            pipeline
            | beam.Create([None])
            | 'LogBeamArgs' >> beam.ParDo(LogPipelineOptionsFn(beam_options, message='Beam Arguments'))
            | 'LogOtherArgs' >> beam.ParDo(LogPipelineOptionsFn(args, message='Other Arguments'))
        )

        _ = (
            pipeline
            | 'CreateMappedMPIPCollection' >> beam.Create(mpi_list)
            | 'VectorizeMPIDocuments' >> beam.ParDo(MPIVectorizer(secret=secret, mpi_collection=mpi_collection))
            | 'UpdateMPIVectorsTable' >> beam.ParDo(MPIVectorTableUpdate(secret=secret))
        )



if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
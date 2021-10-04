"""
firestore_to_bigquery pipeline

An apache beam pipeline to update MPI vectors table from firestore
given context.
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from update.pipeline.local_utils import (
    create_select_mpi_query_from_context,
    create_delete_mpis_from_mpi_list,
    MPIVectorizer,
    MPIVectorTableUpdate
)

from utils.runners import send_query
from utils.pipeline_utils import CustomArgParserFactory, LogPipelineOptionsFn
from gcp.client import get_bigquery_client
from gcp.models import Context

from settings import config
from tests.test_preprocessing import generate_raw_ui_message  ##DEBUG ONLY

import logging

logger = logging.getLogger(__name__)



# Create Pipeline
def run_pipeline(beam_options, args, secret, mpi_collection, mpi_vectors_table, context, save_main_session=True):
    beam_options.view_as(SetupOptions).save_main_session = save_main_session

    # Get list of MPIs to work with.  Only MPIs under consideration need to 
    # have MPI vectors updated
    # TODO: integrate into pipeline for better row handling.  Either fix permissions issues or
    # re-implement streaming from table, chunking, bundling, etc. with given client.  This implementation
    # cannot handle large tables as it will attempt to put a list of all affected MPIs into memory 
    # (ok for a couple thousand, not a couple million)

    bigquery_client = get_bigquery_client(secret=secret)

    mpi_query = create_select_mpi_query_from_context(args, tablename=tablename)
    err, res = send_query(mpi_query, verbose=True, client=bigquery_client)
    if err is not None:
        raise err

    mpi_list = [r.mpi for r in res]

    # Delete all mpi vectors in table where mpi in mpi_list
    mpi_vector_delete_query = create_delete_mpis_from_mpi_list(args, tablename=tablename)
    err, res = send_query(mpi_vector_delete_query, verbose=True, client=bigquery_client)
    if err is not None:
        raise err

    logger.warn('PIPELINE DEFINED')
    with beam.Pipeline(options=beam_options) as pipeline:
        # Add a branch for logging the ValueProvider value.
        _ = (
            pipeline
            | 'Empty-pipeline-args' >> beam.Create([None])
            | 'LogOtherArgs' >> beam.ParDo(LogPipelineOptionsFn(
                options=args, message='Other Arguments', options_type='other'))
        )

        _ = (
            pipeline
            | 'Empty-other-args' >> beam.Create([None])
            | 'LogBeamArgs' >> beam.ParDo(LogPipelineOptionsFn(
                options=beam_options, message='Beam Arguments', options_type='pipeline'))
        )

        _ = (
            pipeline
            | 'CreateMappedMPIPCollection' >> beam.Create(mpi_list)
            | 'VectorizeMPIDocuments' >> beam.ParDo(MPIVectorizer(secret=secret, mpi_collection=mpi_collection))
            | 'UpdateMPIVectorsTable' >> beam.ParDo(MPIVectorTableUpdate(secret=secret, mpi_vectors_table=mpi_vectors_table))
        )



if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    # Setup Pipeline Options & Command Line Arguments
    parser_factory = CustomArgParserFactory()
    parser = parser_factory()
    args, beam_args = parser.parse_known_args()

    ###############################################
    ### Cross Platform Configuration Management ###
    ###############################################
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).


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

    if args.vectable is not None:
        mpi_vectors_table = args.vectable
    else:
        mpi_vectors_table = config.MPI_VECTORS_TABLE

    if args.secret is not None:
        secret = args.secret
    else:
        secret = config.MPI_SERVICE_SECRET_NAME
    
    if args.context is not None:
        if len(args.context.raw) > 5:
            context = args.context
        else:
            context = Context(raw = generate_raw_ui_message())  ## DEGBUG CHECK.  Used for development only - cut out for prod.    
    else:
        context = Context(raw = generate_raw_ui_message())

    if args.bucket is not None:
        bucket = args.bucket
    else:
        bucket = config.GCS_BUCKET_NAME

    GCS_BUCKET_FULL_PATH = 'gs://' + bucket + '/index'

    ####################################
    ### END CONFIGURATION MANAGEMENT ###
    ####################################

    beam_options = PipelineOptions(
        beam_args,
        project=config.GCP_PROJECT_ID,
        temp_location=GCS_BUCKET_FULL_PATH,
        staging_location=GCS_BUCKET_FULL_PATH,
        service_account_email='udrc-mpi-sa@ut-dws-udrc-dev.iam.gserviceaccount.com',
    )
    
    run_pipeline(
        beam_options, args, 
        secret=secret, 
        mpi_collection=mpi_collection, 
        mpi_vectors_table=mpi_vectors_table, 
        context=context
    )
"""
index_table pipeline

Index a preprocessed table to produce a candidate comparison table.
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

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

    ###############################################
    ### Cross Platform Configuration Management ###
    ###############################################

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

    ####################################
    ### END CONFIGURATION MANAGEMENT ###
    ####################################


    logger.info('PIPELINE DEFINED')
    with beam.Pipeline(options=beam_options) as pipeline:
        pass



if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
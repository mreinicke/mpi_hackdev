"""
index_table pipeline

Index a preprocessed table to produce a candidate comparison table.
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.transforms.core import Create
from index.index import BlockIndexer
from index.pipeline import delete_table_if_exists, NameMatchIndexFn
from utils.pipeline_utils import CustomArgParserFactory, LogPipelineOptionsFn
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

    ###############################################
    ### Cross Platform Configuration Management ###
    ###############################################

    if config.DEBUG:
        tablename = config.BIGQUERY_TEST_PREPROCESSED_TABLE
        mapped_columns = ['ssn', 'first_name', 'last_name']
    else:
        tablename = None
        mapped_columns = None
        raise NotImplementedError('Context tablename and mapped columns not implemented for this pipeline')

    if args.vectable is not None:
        mpi_vectors_table = args.vectable
    else:
        mpi_vectors_table = config.MPI_VECTORS_TABLE

    if args.secret is not None:
        secret = args.secret
    else:
        secret = config.MPI_SERVICE_SECRET_NAME

    if args.bucket is not None:
        bucket = args.bucket
    else:
        bucket = config.GCS_BUCKET_NAME

    ####################################
    ### END CONFIGURATION MANAGEMENT ###
    ####################################


    ###########################
    ### Initialize Indexing ###
    ###########################

    ## Delete Indexed Table If Exists ##
    err = delete_table_if_exists(tablename=tablename)
    logger.warning(err)  ## TODO: Expect drop table errrors?

    ## Block Index Table ##
    block_indexer = BlockIndexer(
        mapped_columns=mapped_columns,
        preprocessed_table=tablename,
        mpi_vectors_table=mpi_vectors_table,
        secret=secret,
    )
    err, _ = block_indexer.index()
    assert err is None, err


    ######################
    ### Start Pipeline ###
    ######################


    logger.info('PIPELINE DEFINED')
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

        # NameIndex a preprocessed table
        if ('first_name' in mapped_columns) and ('last_name' in mapped_columns):
            ## Use bigqueryio connector to control data -> required for larger tables
            query = f"SELECT first_name, last_name, rownum FROM `{tablename}`"
            # indexes = (
            #     pipeline
            #     | 'Read Preprocessed' >> beam.io.ReadFromBigQuery(query=query)
            #     | beam.Map(print)
            # )

            err, res = send_query(query, verbose=True)
            if err is not None:
                raise err

            _ = (
                pipeline
                | 'DEBUG: create rows from bigquery' >> beam.Create(
                    [(r.first_name, r.last_name, r.rownum) for r in res])
                | 'Index FirstName/LastName' >> beam.ParDo(NameMatchIndexFn(
                    secret=secret,
                    mapped_columns=mapped_columns,
                    tablename=tablename,
                    bucket=bucket
                ))
            )  

        else:
            logger.info('NameVector indexing skipped.  first_name and last_name not found in mapped_columns.')



if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
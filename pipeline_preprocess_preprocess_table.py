"""
update_search_tree pipeline

Rebuild search tree from MPI Vectors table.
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from preprocess.pipeline.local_utils import PreprocessTableFn
from gcp.models import Context
from tests.test_preprocessing import generate_raw_ui_message
from utils.pipeline_utils import CustomArgParserFactory, LogPipelineOptionsFn
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

    if args.secret is not None:
        secret = args.secret
    else:
        secret = config.MPI_SERVICE_SECRET_NAME

    if args.context is not None:
        context = args.context
    else:
        context = Context(raw = generate_raw_ui_message())

    ####################################
    ### END CONFIGURATION MANAGEMENT ###
    ####################################

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

        # Preprocess Table Pipeline
        _ = (
            pipeline
            | 'Empty-context' >> beam.Create([None])
            | 'Preprocess Table' >> beam.ParDo(PreprocessTableFn(
                context=context,
                secret=secret
            ))
        )




if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
# preprocess.py

from .. gcp.models import Context

from copy import copy

import logging 
logger = logging.getLogger(__name__)

##########################################
### Standard Pre-Processing Transforms ###
##########################################
"""
SQL Template Statements

At runtime, we are unsure which transforms are necessary.
A query is composed via mapping provided by external service
and a final pre-processing query is constructed.
"""

filters = {
    'first_name': "LOWER(TRIM(REGEXP_REPLACE(<mapped_name>, '[^a-zA-Z0-9]', ''))) AS first_name",
    'last_name': "LOWER(TRIM(REGEXP_REPLACE(<mapped_name>, '[^a-zA-Z0-9]', ''))) AS last_name",
    'middle_name': "LOWER(TRIM(REGEXP_REPLACE(<mapped_name>, '[^a-zA-Z0-9]', ''))) AS middle_name",
    'ssn': (
    "CASE "
    "WHEN LENGTH(REGEXP_REPLACE(CAST(<mapped_name> AS STRING), '[^0-9]', '')) != 9 "
    "THEN NULL "
    "ELSE REGEXP_REPLACE(CAST(<mapped_name> AS STRING), '[^0-9]', '') "
    "END AS ssn"
    ),
    'ssid': "<mapped_name> AS ssid",
    'usbe_student_id': "<mapped_name> AS usbe_student_id",
    'ushe_student_id': "<mapped_name> AS ushe_student_id",
    'ustc_student_id': "<mapped_name> AS ustc_student_id",
    'gender': "<mapped_name> AS gender",
    'birth_date': "<mapped_name> AS birth_date",
    'ethnicity': "<mapped_name> AS ethnicity",
}

template_query = (
    "WITH \n"
    "\tsource AS ( \n"
    "\tSELECT * FROM `<tablename>` \n"
    "\t), \n"
    "\tclean AS ( \n"
    "\tSELECT <filters> FROM source \n"
    "\t) \n"
    "SELECT DISTINCT <common_names>, ROW_NUMBER() OVER() AS rownum FROM clean; "
)

def replace_filter_components(colname: str, partner: str, query: str) -> str:
    return query\
            .replace('<mapped_name>', colname)\
            .replace('<partner_id>', partner)


def compose_preprocessing_query(context: Context, template: str = template_query, pretty=False) -> str:
    """compose preprocessing query

    Takes a mapping, partner ID, and table to generate a preprocessing query.

    Args:
        mapping (dict): Of form {'common_name': 'table_column_name'}
        partner (str): sytem partner id
        tablename (str): Fully qualified BigQuery tablename - `project.dataset.tablename`
        template (str, optional): [description]. Defaults to template_query.

    Returns:
        str: composed query with elements replaced with mapping details
    """
    
    def _collect_template_filters(mapping: dict, partner: str) -> tuple:
        try:
            return tuple([replace_filter_components(mapping[k], partner, filters[k]) for k in mapping.keys()])
        except KeyError as e:
            logger.error(f'Invalid Mapping: {e}')
            raise e

    mapping = context.mapping
    partner = context.partner
    tablename = context.source_tablename

    s = copy(template)
    processing_queries = _collect_template_filters(mapping, partner)
    s = s\
        .replace('<filters>', '\t,\n'.join(processing_queries))\
        .replace('<common_names>', ','.join(list(mapping.keys())))\
        .replace('<tablename>', tablename)

    if pretty:
        return s
    else:
        return s.replace('\t', '').replace('\n', '')


def compose_preprocessed_table_query(context: Context):
    # Add the CREATE TABLE statement if making a new table
    tablename = context.source_tablename

    output_table_name = f"{tablename.strip('`')}_preprocessed"
    query = f"CREATE TABLE `{output_table_name}` AS "\
            + compose_preprocessing_query(context)
    return query, output_table_name
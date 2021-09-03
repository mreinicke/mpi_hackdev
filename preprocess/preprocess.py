# preprocess.py

from gcp.client import get_bigquery_client
from copy import copy

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
    'first_name': "UPPER(TRIM(REGEXP_REPLACE(<mapped_name>, '[^a-zA-Z0-9]', ''))) AS first_name",
    'last_name': "UPPER(TRIM(REGEXP_REPLACE(<mapped_name>, '[^a-zA-Z0-9]', ''))) AS last_name",
    'middle_name': "UPPER(TRIM(REGEXP_REPLACE(<mapped_name>, '[^a-zA-Z0-9]', ''))) AS middle_name",
    'ssn': (
    "CASE "
    "WHEN LENGTH(REGEXP_REPLACE(<mapped_name>, '[^0-9]', '')) != 9 "
    "THEN NULL "
    "ELSE REGEXP_REPLACE(<mapped_name>, '[^0-9]', '') "
    "END AS ssn"
    ),
    'ssid': "<mapped_name> AS ssid",
    'student_id': "<mapped_name> AS <partner_id>_student_id",
}

template_query = """

"""

def replace_query_components(colname: str, partner: str, query: str) -> str:
    return query\
            .replace('<mapped_name>', colname)\
            .replace('<partner_id>', partner)


def compose_preprocessing_query(mapping: dict, partner: str, tablename: str, template: str = template_query) -> str:
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
        return tuple([replace_query_components(mapping[k], partner, filters[k]) for k in mapping.keys()])
    
    s = copy(template)
    processing_queries = _collect_template_filters(mapping, partner)
    return processing_queries


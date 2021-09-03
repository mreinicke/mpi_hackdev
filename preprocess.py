# preprocess.py

from gcp.client import get_bigquery_client


first_name_filter = "UPPER(TRIM(REGEXP_REPLACE(<mapped_name>, '[^a-zA-Z0-9]', ''))) AS first_name"
last_name_filter = "UPPER(TRIM(REGEXP_REPLACE(<mapped_name>, '[^a-zA-Z0-9]', ''))) AS last_name"
ssn_filter = (
    "CASE "
    "WHEN LENGTH(REGEXP_REPLACE(<mapped_name>, '[^0-9]', '')) != 9 "
    "THEN NULL "
    "ELSE REGEXP_REPLACE(<mapped_name>, '[^0-9]', '') "
    "END AS ssn"
)
student_id_filter = "<mapped_name> AS <partner_id>_<mapped_name>"

def replace_query_components(colname: str, partner: str, query: str) -> str:
    return query\
            .replace('<mapped_name>', colname)\
            .replace('<partner_id>', partner)
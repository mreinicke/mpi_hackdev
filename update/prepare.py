"""update.prepare

    prepare assets in context for update procedures
"""

from gcp.models import Context
from utils.loaders import load_bigquery_table
from utils.runners import send_query

##########################
### 1. Assign New MPIs ###
##########################

def update_preprocessed_table(context: Context) -> tuple:
    """Update Preprocessed Table
    
        Genereate MPI for unmatched rows.  Set prob_match for those rows to 1.
    """
    err = None
    tablename = context.source_tablename
    # Try to load table
    try:
        load_bigquery_table(tablename)
    except Exception as e:
        return e, tablename

    QUERY = f"""
    UPDATE `{tablename}`
    SET 
        mpi = GENERATE_UUID(),
        prob_match = 1.0
    WHERE mpi is NULL
    """

    err, _ = send_query(QUERY)
    return err, tablename


###########################################
### 2. Delete MPI Vectors Before Update ###
###########################################

def delete_mpi_vectors_in_table(context: Context, tablename: str) -> tuple:
    return NotADirectoryError, None
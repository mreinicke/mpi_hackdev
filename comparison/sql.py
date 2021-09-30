# comparison.sql.py


def build_select_distinct_ind(tablename: str) -> str:
    return f"""
    source_ind AS (
        SELECT DISTINCT rownum, mpi
        FROM `{tablename}_index` 
    )
    """


def build_join_preprocessed(tablename: str, mpi_vectors_table: str) -> str:
    return f"""
    comparsion_cte AS (
        SELECT
            pp.*,
            vec.*
        FROM
            source_ind
            INNER JOIN `{tablename}_preprocessed` pp
                ON source_ind.rownum = pp.rownum
            INNER JOIN `{mpi_vectors_table}` vec
                ON source_ind.mpi = vec.mpi
    )
    """
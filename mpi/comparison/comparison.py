"""
Comparison

Given an index of candidates, create the comparison matrices for 
model classification.  This is done in two steps:

1. Initialize a comparator (construct comparisons via mapped columns)
2. Execute comparator and export comparison table

The result is a a table of the form:
rownum-mpi-[field_comparisons]-frequency_score
"""

from typing import List
from ..utils.iterators import coalesce
from ..comparison.sql import (
    build_select_distinct_ind,
    build_join_preprocessed,
)
from ..settings import config


class Comparator():
    def __init__(self, mapped_columns: List[str], mpi_vectors_table=None, preprocessed_table = None, secret=None, **kwargs) -> None:
        self.mapped_columns = mapped_columns
        self.secret = coalesce(secret, config.MPI_SERVICE_SECRET_NAME)
        self.mpi_vectors_table = coalesce(mpi_vectors_table, config.MPI_VECTORS_TABLE)
        self.preprocessed_table = coalesce(preprocessed_table, config.BIGQUERY_TEST_TABLE)
        for kwarg in kwargs:
            if hasattr(self, kwarg):
                self.kwarg = kwargs[kwarg]
            else:
                setattr(self, kwarg, kwargs[kwarg])

        assert self.secret is not None, 'Must provide secret'
        assert self.mpi_vectors_table is not None, 'Must provide mpi_vectors_table'
        assert self.preprocessed_table is not None, 'Must provide preprocessed_table'


    def assemble_comparison(self):
        SELECT_DISTINCT_IND = build_select_distinct_ind(self.preprocessed_table)
        JOIN_PREPROCESSED = build_join_preprocessed(self.preprocessed_table, self.mpi_vectors_table)
        return f"""
        WITH
        {SELECT_DISTINCT_IND},
        {JOIN_PREPROCESSED}
        SELECT * FROM comparsion_cte;
        """

    
    def run(self):
        raise NotImplementedError('must implement method')
"""
Model mapping for validated structs
"""



import logging
logger = logging.getLogger(__name__)

from pydantic import BaseModel
from typing import List, Dict

from config import ALLOWED_PII

#################
###Data Models###
#################



##################################
### NOSQL (Firestore) Template ###
##################################

# Reference Model
firestore_model = {
    "mpi": str,  # DocumentID == mpi
    "sources": [
        {
            "guid": str,
            "fields": {"fieldname": str, "value": None},
            "prob_match": float,
        }
    ]
}


# Typed models derived from refernce model
class SourceRecord(BaseModel):
    guid: str
    prob_match: float
    fields: dict


class MPIRecord(BaseModel):
    mpi: str 
    sources: List[SourceRecord]



## NoSQL Utility ##
# Build a serializer to convert from matched row <mpi + PII data + score>
def filter_dict_for_allowed_pii(d: dict, allowed=ALLOWED_PII) -> dict:
    def _is_allowed(k: str) -> bool:
        return k in allowed
    
    nd = {k:d[k] for k in filter(_is_allowed, d.keys())}
    return nd


def build_source_record_from_row(row: dict, context: dict) -> SourceRecord:
    guid = context['guid']
    prob_match = row['prob_match']
    fields = filter_dict_for_allowed_pii(row)
    return SourceRecord(
        guid=guid,
        prob_match=prob_match,
        fields=fields,
    )


def build_mpi_record_from_row(row: dict, context: dict) -> MPIRecord:
    mpi = row['mpi']
    sources = [build_source_record_from_row(row, context)]
    return MPIRecord(
        mpi=mpi,
        sources=sources
    )


class NoSQLSerializer():

    def __init__(self, context: dict):
        self.context = context

    def _check_row_context(self, row):
        assert 'mpi' in row, 'Cannot marshal. Missing MPI in expected key group.'
        assert 'guid' in self.context, 'Cannot marshal.  Missing GUID in expected key group.'
        return row

    def _marshal(self, row):
        return build_mpi_record_from_row(
            row=row,
            context=self.context
        )

    def __call__(self, raw):
        return self._marshal(self._check_row_context(raw))
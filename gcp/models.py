"""
Model mapping for validated structs
"""

import logging
logger = logging.getLogger(__name__)

from pydantic import BaseModel
from typing import List, Optional
import json
import time

from utils.embeds import AlphabetVectorizer

from settings import config

#################
###Data Models###
#################

v = AlphabetVectorizer()

class MPIVector(BaseModel):
    """Data model and serialization methods for BigQuery MPI_VECTORS table"""
    mpi: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    ssn: Optional[int] = None
    ssid: Optional[str] = None
    middle_name: Optional[str] = None
    ushe_student_id: Optional[str] = None
    usbe_student_id: Optional[str] = None
    gender: Optional[str] = None
    ethnicity: Optional[str] = None
    birth_date: Optional[str] = None
    frequency_score: Optional[float] = None

    @property
    def name_match_vector(self):
        if (self.first_name is not None) and (self.last_name is not None):
            vector = v(self.first_name + self.last_name)
            return '-'.join([str(v) for v in vector])
        return None

    def as_sql(self, tablename=config.MPI_VECTORS_TABLE):
        def _value_or_null(n):
            v = getattr(self, n)
            if v is not None:
                if type(v) == str:
                    return f"'{v}'"
                return str(v)
            return 'NULL'

        columns_list = [
            'mpi', 'first_name', 'last_name', 'ssn', 'ssid',
            'middle_name', 'ushe_student_id', 'usbe_student_id',
            'gender', 'ethnicity', 'birth_date', 'frequency_score', 
            'name_match_vector'
        ]

        return f"""
        INSERT INTO `{tablename}` ({','.join(columns_list)})
        VALUES ({','.join([_value_or_null(c) for c in columns_list])})
        """


class Context(BaseModel):
    raw: str
    parsed: Optional[dict] = None

    def load_raw(self):
        if self.parsed is None:
            self.parsed = json.loads(self.raw)
        return self.parsed

    @property
    def guid(self) -> str:
        return self.load_raw()['guid']

    @property
    def source_tablename(self) -> str:
        return self.load_raw()['sourceTable']

    @property
    def destination_tablename(self) -> str:
        return self.load_raw()['destination']

    @property
    def partner(self) -> str:
        return self.load_raw()['partner']

    @property 
    def mapping(self):
        def _filter_mapped_columns(columns: list):
            temp = {}
            for column in columns:
                if 'MPI' in column['outputs'].keys():
                    table_column_name = column['name']
                    common_name = column['outputs']['MPI']['name']
                    temp[common_name] = table_column_name
            return temp

        return _filter_mapped_columns(self.load_raw()['columns'])

    def __get_item__(self, key):
        return getattr(self, key)
                    




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

    def as_dict(self):
        d = dict(self)
        d['sources'] = [dict(s) for s in d['sources']]
        d['updated'] = time.time()
        return d


## NoSQL Utility ##
# Build a serializer to convert from matched row <mpi + PII data + score>
def filter_dict_for_allowed_pii(d: dict, allowed=config.ALLOWED_PII) -> dict:
    def _is_allowed(k: str) -> bool:
        return k in allowed
    
    nd = {k:d[k] for k in filter(_is_allowed, d.keys())}
    return nd


def build_source_record_from_row(row: dict, context: Context) -> SourceRecord:
    guid = context.guid
    prob_match = row['prob_match']
    fields = filter_dict_for_allowed_pii(row)
    return SourceRecord(
        guid=guid,
        prob_match=prob_match,
        fields=fields,
    )


def build_mpi_record_from_row(row: dict, context: Context) -> MPIRecord:
    mpi = row['mpi']
    sources = [build_source_record_from_row(row, context)]
    return MPIRecord(
        mpi=mpi,
        sources=sources
    )


class NoSQLSerializer():
    """Convert BigQuery row to Firestore NoSQL model"""
    def __init__(self, context: dict):
        self.context = context

    def _check_row_context(self, row):
        assert 'mpi' in row, 'Cannot marshal. Missing MPI in expected key group.'
        assert len(self.context.guid) > 0, 'Cannot marshal.  Missing GUID in expected key group.'
        return row

    def _marshal(self, row):
        return build_mpi_record_from_row(
            row=row,
            context=self.context
        )

    def __call__(self, row):
        return self._marshal(self._check_row_context(row))
"""Index

Find candidate matches for each row where available.
"""

"""Index

    Search nearest neighbors and blocked candidates for later comparison
    and classification.


    name_index <= SearchTree(first_name+last_name)
        *middle name not indexed
        *first name/last name will not be searched 

"""
from google.cloud.firestore_v1.base_collection import BaseCollectionReference
from gcp.client import get_firestore_client

from typing import List

from settings import config
import logging

logger = logging.getLogger(__name__)



class Indexer():
    def __init__(self, mapped_columns: List[str]) -> None:
        self.mapped_columns = mapped_columns

    def assemble_search(self):
        raise NotImplementedError('must implement method')

    def index(self):
        raise NotImplementedError('must implement method')



class BlockIndexer(Indexer):
    """
    Block Indexer
    
    Assembles queries to screen entire table on exact matches given blocked_index_allow 
    flag.
    """
    def __init__(self, mapped_columns: List[str]) -> None:
        super().__init__(mapped_columns)
        self.__search_tree = None
        self.search = self.assemble_search()
        self.block_index_allow = [
            'ssn', 'ssid', 'birth_date', 'ushe_student_id',
            'usbe_student_id', 'ustc_student_id'
        ]


    @property
    def search_tree(self):
        if self.__search_tree is None:
            self.__search_tree = get_search_tree()
        return self.__search_tree


    def assemble_search(self):
        pass


    def index(self, tablename: str):
        return self.search(tablename)



class DemographicIndexer():
    pass




# Utilities to assist in loading index assets and setup
def get_search_tree():
    pass


def get_default_collection() -> BaseCollectionReference:
    db = get_firestore_client()
    ## TODO: does collection exist?
    return db.collection(config.FIRESTORE_IDENTITY_POOL)


def is_collection_length_large(minsize=10) -> bool:
    """Run a limited query to make sure there are some identities in the pool"""
    collection = get_default_collection()
    counts = [1 for d in collection.where('frequency_score',  '>', 0.1).limit(minsize).stream()]
    return sum(counts) >= minsize


def check_cold_start():
    return is_collection_length_large()


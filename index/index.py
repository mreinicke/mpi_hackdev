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

import config
import logging

logger = logging.getLogger(__name__)



class BlockIndexer():
    
    def __init__(self, mapped_columns: list) -> None:
        self.mapped_columns = mapped_columns
        self.search_tree_ = None
        self.search = self.assemble_search()

    
    @property
    def search_tree(self):
        if self.search_tree_ is None:
            self.search_tree_ = get_search_tree()
        return self.search_tree_


    def assemble_search(self):
        pass


    def index(self, row):
        return self.search(row)



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


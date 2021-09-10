"""
Batch

Helper containers and methods for handling batches of objects.
Built to assist in filtering records and routing to bulk writes
in firestore or bigquery APIs.
"""


class Batch():
    def __init__(self, max_size=100, filterfn=None) -> None:
        self.__store = []
        self.max_size = max_size
        self.filterfn = filterfn

    def add(self, value):
        if len(self.__store) < self.max_size:
            self.__store.append(value)
            return True
        return False 
    
    def get(self):
        return self.__store

    def filter(self, *args):
        return self.filterfn(self.get(), *args)

    def flush(self):
        self.__store = []

    @property
    def full(self):
        return len(self.__store) == self.max_size

    @property
    def size(self):
        return len(self.__store)




class BatchFilter():
    """BatchFilter
        Sort and tag items in a batch.
    """
    def __init__(self) -> None:
        pass

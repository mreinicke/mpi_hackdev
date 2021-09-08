#embeds.py

import string
import numpy as np


import logging

logger = logging.getLogger(__name__)



class AlphabetVectorizer():
    def __init__(self, index=None):
        if index is None:
            logger.debug('Initializing AlphabetVectorizer with default index.')
            index = {l: i for i, l in enumerate(string.ascii_lowercase)}
        
        self.index = index
        self.index_len = len(index)
    
    def init_vector(self):
        """ init_vector 
            
            Initialize a zeros vector and position encoding vector
            
            The positional encoding vector can be created linearly or a sigmoid may 
            fit to the length of the index. 
        """
        return np.zeros(len(self.index)), np.arange(0.01, (len(self.index)+1)*0.01, 0.01)
    
    
    def transform(self, x: str) -> np.ndarray:
        def scale_position(pos, x_size):
            return round(pos / x_size * self.index_len)
        
        def update_vector(val, vector, pvector, i):
            if val in self.index:
                
                vector[self.index[val]] += 1 * pvector[i]  ## We add positional information into the vector here
        
        v, p = self.init_vector()
        x_len = len(x)
        for i, val in enumerate(x):
            i = scale_position(i, x_len)  ## Scale position of the letters to position vector size.
            update_vector(val, v, p, i)
        
        return v
        
    
    def __call__(self, x: str):
        return self.transform(x)
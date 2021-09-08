"""
Model mapping for validated structs
"""

import logging
logger = logging.getLogger(__name__)



#################
###Data Models###
#################



################################
### NOSQL (Firestore) Template ###
################################


mongo_model = {
    "mpi": str,
    "sources": [
        {
            "guid": int,
            "fields": [
                {
                    "fieldname": str,
                    "value": str,
                }
            ],
            "score": float,
        }
    ]
}


## NoSQL Utility ##
class Validator():
    def __init__(self, expected=None, query=None):
        self.expected = expected
        self.query = query

    def _check_expected(self, x):
        if self.expected == 'any':
            return x is not None 
        return x == self.expected

    def _query(self, data):
        def _int(x):
            try:
                return int(x)
            except:
                return x
        val = data
        for cond in self.query.split('.'):
            try:
                val = val[_int(cond)]
            except Exception as e:
                logger.error(f"Failed query: {val}, {cond}, {e}")
        return type(val)  # type validation

    def test(self, data):
        result = self._check_expected(self._query(data))
        if result == False:
            logger.error(f"failed: { self.query}")
        return result


# Define model validators for each field
validators = (
    Validator(query='mpi', expected='any'),
    Validator(query='sources', expected=list),
    Validator(query='sources.0.guid', expected=int),
    Validator(query='sources.0.fields', expected=list),
    Validator(query='sources.0.fields.0.fieldname', expected=str),
    Validator(query='sources.0.fields.0.value', expected='any'),
    Validator(query='sources.0.score', expected=float)
)


def validate_model(data, validators=validators):
    
    def _run_validator(validator, data=data):
        return validator.test(data)

    # logger.debug(f"{list(map(_run_validator, validators))}, {len(validators)}")
    return sum(list(map(_run_validator, validators))) == len(validators)


# Build a serializer to convert from raw vector

class NoSQLSerializer():

    def __init__(self, validator=validate_model):
        self.validator = validate_model


    def _validate_doc(self, document):
        if self.validator(document):
            return document
        else:
            raise ValueError(f'Invalid document created during serialization.  Check:\n{document}')

    def _check_raw(self, raw):
        assert 'mpi' in raw, 'Cannot marshal. Missing MPI in expected key group.'
        assert 'guid' in raw, 'Cannot marshal.  Missing GUID in expected key group.'
        return raw


    def _marshal(self, raw):
        def _is_field(x):
            return x not in ['mpi', 'guid', 'score']

        mpi = raw['mpi']
        guid = raw['guid']
        if 'score' in raw:
            score = raw['score']
        else:
            score = 0.00
        return {
            'mpi': mpi,
            'sources': [
                {
                    'guid': guid,
                    'score': score,
                    'fields': [
                        {'fieldname': key, 'value': raw[key]} for key in raw if _is_field(key)]
                }
            ]
        }


    def __call__(self, raw):
        return self._validate_doc(
            self._marshal(self._check_raw(raw))
        ) 
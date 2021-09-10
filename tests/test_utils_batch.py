# test_utils_batch.py

import pytest 

from utils.batch import Batch


@pytest.fixture 
def test_filter():
    def _filter_fn(rows: list, *args):
        filtered = {
            'a': [item for item in rows if item['key'] == 'to_a'],
            'b': [item for item in rows if item['key'] == 'to_b'],
        }

        for a in args:
            assert a in filtered.keys(), f'Invalid filter order {a}'
    
        return [filtered[a] for a in args]

    return _filter_fn



def test_batch_fill():
    batch = Batch(max_size=10)
    for i in range(10):
        batch.add(i)

    assert batch.size == 10
    assert batch.full == True
    assert batch.add(1) == False


def test_batch_filter(test_filter):
    rows = [{'key': 'to_a', 'prop': 'first'}, {'key': 'to_a', 'prop': 'second'}, {'key': 'to_b', 'prop': 'third'}]
    a, b = test_filter(rows, 'a', 'b')
    assert len(a) == 2
    assert len(b) == 1
    batch = Batch(max_size=100, filterfn=test_filter)
    [batch.add(row) for row in rows]
    a, b = batch.filter('a', 'b')
    assert len(a) == 2
    assert len(b) == 1
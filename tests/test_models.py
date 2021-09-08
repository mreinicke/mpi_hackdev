# test_models.py


import pytest

from uuid import uuid4
from random import choice

@pytest.fixture
def example_data():
    rows = [
        {},
        {},
        ]
    context = {
        'guid': str(uuid4()),
        'partner': choice(['USHE', 'USBE', 'UDOH', 'ADHOC', 'USTC'])
    }



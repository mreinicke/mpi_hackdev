# iterators.py


def coalesce(*args):
    for a in args:
        if a is not None:
            return a
from itertools import tee


def pairwise(iterable):
    """
    iterable -> (s0,s1), (s1,s2), (s2, s3), ...
    """
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def iter_show_end(iterable):
    """
    iterable -> (s0, false), (s1, false), (s2, true).
    """
    it = iter(iterable)
    last = next(it)
    for val in it:
        yield last, False
        last = val
    yield last, True

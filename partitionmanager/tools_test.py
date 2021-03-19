import unittest

from .tools import pairwise


class TestTools(unittest.TestCase):
    def test_pairwise(self):
        self.assertEqual(list(pairwise(["a", "b"])), [("a", "b")])
        self.assertEqual(list(pairwise(["a", "b", "c"])), [("a", "b"), ("b", "c")])
        self.assertEqual(list(pairwise(["a"])), [])

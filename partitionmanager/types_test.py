import argparse
import unittest
from .types import toSqlUrl


class TestTypes(unittest.TestCase):
    def test_dburl_invalid(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            toSqlUrl("http://localhost/dbname")

    def test_dburl_without_db_path(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            toSqlUrl("sql://localhost")
        with self.assertRaises(argparse.ArgumentTypeError):
            toSqlUrl("sql://localhost/")

    def test_dburl_with_two_passwords(self):
        u = toSqlUrl("sql://username:password:else@localhost:3306/database")
        self.assertEqual(u.username, "username")
        self.assertEqual(u.password, "password:else")
        self.assertEqual(u.port, 3306)

    def test_dburl_with_port(self):
        u = toSqlUrl("sql://localhost:3306/database")
        self.assertEqual(u.hostname, "localhost")
        self.assertEqual(u.username, None)
        self.assertEqual(u.password, None)
        self.assertEqual(u.port, 3306)

    def test_dburl_with_no_port(self):
        u = toSqlUrl("sql://localhost/database")
        self.assertEqual(u.hostname, "localhost")
        self.assertEqual(u.username, None)
        self.assertEqual(u.password, None)
        self.assertEqual(u.port, None)

    def test_dburl_with_user_pass_and_no_port(self):
        u = toSqlUrl("sql://username:password@localhost/database")
        self.assertEqual(u.hostname, "localhost")
        self.assertEqual(u.username, "username")
        self.assertEqual(u.password, "password")
        self.assertEqual(u.port, None)

    def test_dburl_with_user_pass_and_port(self):
        u = toSqlUrl("sql://username:password@localhost:911/database")
        self.assertEqual(u.hostname, "localhost")
        self.assertEqual(u.username, "username")
        self.assertEqual(u.password, "password")
        self.assertEqual(u.port, 911)

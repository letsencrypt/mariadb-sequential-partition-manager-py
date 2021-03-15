import argparse
import unittest
from datetime import datetime, timedelta, timezone
from .types import (
    MaxValuePartition,
    PositionPartition,
    retention_from_dict,
    SqlInput,
    Table,
    toSqlUrl,
    UnexpectedPartitionException,
)


def mkPPart(name, *pos):
    p = PositionPartition(name)
    for x in pos:
        p.add_position(x)
    return p


def mkTailPart(name, count=1):
    return MaxValuePartition(name, count)


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

    def test_table(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            Table("invalid'name")

        self.assertEqual(type(Table("name").name), SqlInput)

        t = Table("t")
        self.assertEqual(None, t.retention)

        with self.assertRaises(argparse.ArgumentTypeError):
            retention_from_dict({"something": 1})

        with self.assertRaises(argparse.ArgumentTypeError):
            retention_from_dict({"another thing": 1, "days": 30})

        r = retention_from_dict(dict())
        self.assertEqual(None, r)

        with self.assertRaises(TypeError):
            retention_from_dict({"days": "thirty"})

        r = retention_from_dict({"days": 30})
        self.assertEqual(timedelta(days=30), r)


class TestPartition(unittest.TestCase):
    def test_partition_timestamps(self):
        self.assertIsNone(PositionPartition("").timestamp())
        self.assertIsNone(PositionPartition("not_a_date").timestamp())
        self.assertIsNone(PositionPartition("p_202012310130").timestamp())
        self.assertEqual(
            PositionPartition("p_20201231").timestamp(),
            datetime(2020, 12, 31, tzinfo=timezone.utc),
        )

        self.assertLess(mkPPart("a", 10, 10), mkTailPart("b", count=2))
        with self.assertRaises(UnexpectedPartitionException):
            mkPPart("a", 10, 10) < mkTailPart("b", count=1)

        self.assertFalse(mkPPart("a", 10, 10) < mkPPart("b", 11, 10))
        self.assertFalse(mkPPart("a", 10, 10) < mkPPart("b", 10, 11))
        self.assertLess(mkPPart("a", 10, 10), mkPPart("b", 11, 11))
        self.assertFalse(mkPPart("a", 10, 10) < [10, 11])
        self.assertFalse(mkPPart("a", 10, 10) < [11, 10])
        self.assertLess(mkPPart("a", 10, 10), [11, 11])

        with self.assertRaises(UnexpectedPartitionException):
            mkPPart("a", 10, 10) < mkPPart("b", 11, 11, 11)
        with self.assertRaises(UnexpectedPartitionException):
            mkPPart("a", 10, 10, 10) < mkPPart("b", 11, 11)

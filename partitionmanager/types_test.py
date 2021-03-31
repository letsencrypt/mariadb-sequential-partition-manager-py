import argparse
import unittest
from datetime import date, datetime, timedelta, timezone
from .types import (
    ChangedPartition,
    InstantPartition,
    MaxValuePartition,
    NewPartition,
    PositionPartition,
    retention_from_dict,
    SqlInput,
    Table,
    toSqlUrl,
    UnexpectedPartitionException,
)


def mkPPart(name, *pos):
    return PositionPartition(name).set_position(pos)


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

    def test_changed_partition(self):
        with self.assertRaises(ValueError):
            ChangedPartition("bob")

        with self.assertRaises(ValueError):
            ChangedPartition(PositionPartition("p_20201231")).set_position(2)

        with self.assertRaises(UnexpectedPartitionException):
            ChangedPartition(PositionPartition("p_20210101")).set_position([1, 2, 3, 4])

        c = ChangedPartition(PositionPartition("p_20210101").set_position([1, 2, 3, 4]))
        c.set_timestamp(date(2021, 1, 2))
        y = c.set_position([10, 10, 10, 10])
        self.assertEqual(c, y)

        self.assertEqual(c.timestamp(), date(2021, 1, 2))
        self.assertEqual(c.positions, [10, 10, 10, 10])

        self.assertEqual(
            c.as_partition(),
            PositionPartition("p_20210102").set_position([10, 10, 10, 10]),
        )

        c_max = ChangedPartition(MaxValuePartition("p_20210101", count=1)).set_position(
            [1949]
        )
        self.assertEqual(c_max.timestamp(), datetime(2021, 1, 1, tzinfo=timezone.utc))
        self.assertEqual(c_max.positions, [1949])

        self.assertEqual(
            ChangedPartition(
                PositionPartition("p_20210101").set_position([1, 2, 3, 4])
            ),
            ChangedPartition(
                PositionPartition("p_20210101").set_position([1, 2, 3, 4])
            ),
        )

        self.assertEqual(
            ChangedPartition(
                PositionPartition("p_20210101").set_position([1, 2, 3, 4])
            ).set_important(),
            ChangedPartition(
                PositionPartition("p_20210101").set_position([1, 2, 3, 4])
            ).set_important(),
        )

        self.assertNotEqual(
            ChangedPartition(
                PositionPartition("p_20210101").set_position([1, 2, 4, 4])
            ),
            ChangedPartition(
                PositionPartition("p_20210101").set_position([1, 2, 3, 4])
            ),
        )

        self.assertNotEqual(
            ChangedPartition(
                PositionPartition("p_20210101").set_position([1, 2, 3, 4])
            ).set_important(),
            ChangedPartition(
                PositionPartition("p_20210101").set_position([1, 2, 3, 4])
            ),
        )

        self.assertNotEqual(
            ChangedPartition(
                PositionPartition("p_20210101").set_position([1, 2, 3, 4])
            ),
            ChangedPartition(
                PositionPartition("p_20210102").set_position([1, 2, 3, 4])
            ),
        )

    def test_new_partition(self):
        with self.assertRaises(ValueError):
            NewPartition().as_partition()

        with self.assertRaises(ValueError):
            NewPartition().set_timestamp(date(2021, 12, 31)).as_partition()

        self.assertEqual(
            NewPartition()
            .set_position([3])
            .set_timestamp(date(2021, 12, 31))
            .as_partition(),
            PositionPartition("p_20211231").set_position([3]),
        )

        self.assertEqual(
            NewPartition()
            .set_position([1, 1, 1])
            .set_timestamp(date(1994, 1, 1))
            .as_partition(),
            PositionPartition("p_19940101").set_position([1, 1, 1]),
        )

        self.assertEqual(
            NewPartition().set_position([3]).set_timestamp(date(2021, 12, 31)),
            NewPartition().set_position([3]).set_timestamp(date(2021, 12, 31)),
        )


class TestPartition(unittest.TestCase):
    def test_partition_timestamps(self):
        self.assertIsNone(PositionPartition("").timestamp())
        self.assertIsNone(PositionPartition("not_a_date").timestamp())
        self.assertIsNone(PositionPartition("p_202012310130").timestamp())
        self.assertEqual(
            PositionPartition("p_20201231").timestamp(),
            datetime(2020, 12, 31, tzinfo=timezone.utc),
        )

        self.assertLess(mkPPart("a", 9), mkPPart("b", 11))
        self.assertLess(mkPPart("a", 10), mkPPart("b", 11))
        self.assertFalse(mkPPart("a", 11) < mkPPart("b", 11))
        self.assertFalse(mkPPart("a", 12) < mkPPart("b", 11))

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

    def test_instant_partition(self):
        now = datetime.utcnow()

        ip = InstantPartition(now, [1, 2])
        self.assertEqual(ip.positions, [1, 2])
        self.assertEqual(ip.name, "Instant")
        self.assertEqual(ip.timestamp(), now)

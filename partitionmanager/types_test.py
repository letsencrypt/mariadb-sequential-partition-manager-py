import argparse
import unittest
import pytest
from datetime import datetime, timedelta, timezone
from .types import (
    ChangePlannedPartition,
    InstantPartition,
    is_partition_type,
    MaxValuePartition,
    NewPlannedPartition,
    Position,
    PositionPartition,
    timedelta_from_dict,
    SqlInput,
    SqlQuery,
    Table,
    to_sql_url,
    UnexpectedPartitionException,
)


def mkPos(*pos):
    p = Position()
    p.set_position(pos)
    return p


def mkPPart(name, *pos):
    return PositionPartition(name).set_position(mkPos(*pos))


def mkTailPart(name, count=1):
    return MaxValuePartition(name, count)


class TestSqlQuery(unittest.TestCase):
    def test_multiple_statements(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            SqlQuery("SELECT 'id' FROM 'place' WHERE 'id'=?; SELECT 1=1;")

    def test_multiple_arguments(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            SqlQuery("SELECT 'id' FROM 'place' WHERE 'id'=? OR 'what'=?;")

    def test_forbidden_terms(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            SqlQuery("DELETE FROM 'place';")
        with self.assertRaises(argparse.ArgumentTypeError):
            SqlQuery("UPDATE 'place';")
        with self.assertRaises(argparse.ArgumentTypeError):
            SqlQuery("INSERT INTO 'place';")
        with self.assertRaises(argparse.ArgumentTypeError):
            SqlQuery("ANALYZE 'place';")
        with self.assertRaises(argparse.ArgumentTypeError):
            SqlQuery("SET 'place';")
        with self.assertRaises(argparse.ArgumentTypeError):
            SqlQuery(";")

    def test_get_statement_errors(self):
        q = SqlQuery("SELECT 'id' FROM 'place' WHERE 'id'=?;")
        with self.assertRaises(argparse.ArgumentTypeError):
            q.get_statement_with_argument("must be a SqlInput type")
        with self.assertRaises(argparse.ArgumentTypeError):
            q.get_statement_with_argument(5)
        with self.assertRaises(argparse.ArgumentTypeError):
            q.get_statement_with_argument(None)

    def test_get_statement_string(self):
        q = SqlQuery("SELECT 'id' FROM 'place' WHERE 'status'=?;")

        with self.assertRaises(argparse.ArgumentTypeError):
            q.get_statement_with_argument(SqlInput("strings aren't allowed"))

    def test_get_statement_number(self):
        q = SqlQuery("SELECT 'id' FROM 'place' WHERE 'id'=?;")

        self.assertEqual(
            q.get_statement_with_argument(SqlInput(5)),
            "SELECT 'id' FROM 'place' WHERE 'id'=5;",
        )
        self.assertEqual(
            q.get_statement_with_argument(SqlInput(5555)),
            "SELECT 'id' FROM 'place' WHERE 'id'=5555;",
        )

    def test_get_statement_number_with_newlines(self):
        q = SqlQuery(
            """
                        SELECT 'multilines' FROM 'where it might be' WHERE 'id'=?;
        """
        )
        self.assertEqual(
            q.get_statement_with_argument(SqlInput(0xFF)),
            "SELECT 'multilines' FROM 'where it might be' WHERE 'id'=255;",
        )


class TestTypes(unittest.TestCase):
    def test_dburl_invalid(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            to_sql_url("http://localhost/dbname")

    def test_dburl_without_db_path(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            to_sql_url("sql://localhost")
        with self.assertRaises(argparse.ArgumentTypeError):
            to_sql_url("sql://localhost/")

    def test_dburl_with_two_passwords(self):
        u = to_sql_url("sql://username:password:else@localhost:3306/database")
        self.assertEqual(u.username, "username")
        self.assertEqual(u.password, "password:else")
        self.assertEqual(u.port, 3306)

    def test_dburl_with_port(self):
        u = to_sql_url("sql://localhost:3306/database")
        self.assertEqual(u.hostname, "localhost")
        self.assertEqual(u.username, None)
        self.assertEqual(u.password, None)
        self.assertEqual(u.port, 3306)

    def test_dburl_with_no_port(self):
        u = to_sql_url("sql://localhost/database")
        self.assertEqual(u.hostname, "localhost")
        self.assertEqual(u.username, None)
        self.assertEqual(u.password, None)
        self.assertEqual(u.port, None)

    def test_dburl_with_user_pass_and_no_port(self):
        u = to_sql_url("sql://username:password@localhost/database")
        self.assertEqual(u.hostname, "localhost")
        self.assertEqual(u.username, "username")
        self.assertEqual(u.password, "password")
        self.assertEqual(u.port, None)

    def test_dburl_with_user_pass_and_port(self):
        u = to_sql_url("sql://username:password@localhost:911/database")
        self.assertEqual(u.hostname, "localhost")
        self.assertEqual(u.username, "username")
        self.assertEqual(u.password, "password")
        self.assertEqual(u.port, 911)

    def test_table(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            Table("invalid'name")

        self.assertEqual(type(Table("name").name), SqlInput)

        t = Table("t")
        self.assertEqual(None, t.retention_period)

        self.assertEqual(
            Table("a").set_partition_period(timedelta(days=9)).partition_period,
            timedelta(days=9),
        )

        self.assertEqual(
            Table("a").set_retention_period(timedelta(days=9)).retention_period,
            timedelta(days=9),
        )

        with self.assertRaises(argparse.ArgumentTypeError):
            timedelta_from_dict({"something": 1})

        with self.assertRaises(argparse.ArgumentTypeError):
            timedelta_from_dict({"another thing": 1, "days": 30})

        r = timedelta_from_dict({})
        self.assertEqual(None, r)

        with self.assertRaises(TypeError):
            timedelta_from_dict({"days": "thirty"})

        r = timedelta_from_dict({"days": 30})
        self.assertEqual(timedelta(days=30), r)

        with self.assertRaises(ValueError):
            t.set_earliest_utc_timestamp_query("col")
        with self.assertRaises(ValueError):
            t.set_earliest_utc_timestamp_query(None)
        self.assertFalse(t.has_date_query)

        t.set_earliest_utc_timestamp_query(
            SqlQuery("SELECT not_before FROM table WHERE id = ?;")
        )
        self.assertTrue(t.has_date_query)

    def test_invalid_timedelta_string(self):
        with pytest.raises(AttributeError):
            assert timedelta_from_dict("30s")

    def test_changed_partition(self):
        with self.assertRaises(ValueError):
            ChangePlannedPartition("bob")

        with self.assertRaises(ValueError):
            ChangePlannedPartition(PositionPartition("p_20201231")).set_position(2)

        with self.assertRaises(UnexpectedPartitionException):
            ChangePlannedPartition(PositionPartition("p_20210101")).set_position(
                [1, 2, 3, 4]
            )

        c = ChangePlannedPartition(
            PositionPartition("p_20210101").set_position([1, 2, 3, 4])
        )
        self.assertFalse(c.has_modifications)
        c.set_timestamp(datetime(2021, 1, 2, tzinfo=timezone.utc))
        y = c.set_position([10, 10, 10, 10])
        self.assertEqual(c, y)
        self.assertTrue(c.has_modifications)

        self.assertEqual(c.timestamp(), datetime(2021, 1, 2, tzinfo=timezone.utc))
        self.assertEqual(c.position.as_list(), [10, 10, 10, 10])

        self.assertEqual(
            c.as_partition(),
            PositionPartition("p_20210102").set_position([10, 10, 10, 10]),
        )

        c_max = ChangePlannedPartition(
            MaxValuePartition("p_20210101", count=1)
        ).set_position([1949])
        self.assertEqual(c_max.timestamp(), datetime(2021, 1, 1, tzinfo=timezone.utc))
        self.assertEqual(c_max.position.as_list(), [1949])

        self.assertEqual(
            ChangePlannedPartition(
                PositionPartition("p_20210101").set_position([1, 2, 3, 4])
            ),
            ChangePlannedPartition(
                PositionPartition("p_20210101").set_position([1, 2, 3, 4])
            ),
        )

        self.assertEqual(
            ChangePlannedPartition(
                PositionPartition("p_20210101").set_position([1, 2, 3, 4])
            ).set_important(),
            ChangePlannedPartition(
                PositionPartition("p_20210101").set_position([1, 2, 3, 4])
            ).set_important(),
        )

        self.assertNotEqual(
            ChangePlannedPartition(
                PositionPartition("p_20210101").set_position([1, 2, 4, 4])
            ),
            ChangePlannedPartition(
                PositionPartition("p_20210101").set_position([1, 2, 3, 4])
            ),
        )

        self.assertNotEqual(
            ChangePlannedPartition(
                PositionPartition("p_20210101").set_position([1, 2, 3, 4])
            ).set_important(),
            ChangePlannedPartition(
                PositionPartition("p_20210101").set_position([1, 2, 3, 4])
            ),
        )

        self.assertNotEqual(
            ChangePlannedPartition(
                PositionPartition("p_20210101").set_position([1, 2, 3, 4])
            ),
            ChangePlannedPartition(
                PositionPartition("p_20210102").set_position([1, 2, 3, 4])
            ),
        )
        self.assertEqual(
            ChangePlannedPartition(
                PositionPartition("p_20210101").set_position([1, 2, 3, 4])
            )
            .set_as_max_value()
            .as_partition(),
            NewPlannedPartition()
            .set_columns(4)
            .set_timestamp(datetime(2021, 1, 1, tzinfo=timezone.utc))
            .as_partition(),
        )

    def test_new_partition(self):
        with self.assertRaises(ValueError):
            NewPlannedPartition().as_partition()

        self.assertEqual(
            NewPlannedPartition()
            .set_columns(5)
            .set_timestamp(
                datetime(2021, 12, 31, hour=23, minute=15, tzinfo=timezone.utc)
            )
            .as_partition(),
            MaxValuePartition("p_20211231", count=5),
        )

        self.assertFalse(NewPlannedPartition().has_modifications)

        self.assertEqual(
            NewPlannedPartition()
            .set_position([3])
            .set_timestamp(datetime(2021, 12, 31, tzinfo=timezone.utc))
            .as_partition(),
            PositionPartition("p_20211231").set_position(mkPos(3)),
        )

        self.assertEqual(
            NewPlannedPartition()
            .set_position([1, 1, 1])
            .set_timestamp(datetime(1994, 1, 1, tzinfo=timezone.utc))
            .as_partition(),
            PositionPartition("p_19940101").set_position([1, 1, 1]),
        )

        self.assertEqual(
            NewPlannedPartition()
            .set_position([3])
            .set_timestamp(datetime(2021, 12, 31, tzinfo=timezone.utc)),
            NewPlannedPartition()
            .set_position([3])
            .set_timestamp(datetime(2021, 12, 31, tzinfo=timezone.utc)),
        )

        self.assertEqual(
            NewPlannedPartition()
            .set_position([99, 999])
            .set_timestamp(
                datetime(2021, 12, 31, hour=19, minute=2, tzinfo=timezone.utc)
            )
            .set_as_max_value(),
            NewPlannedPartition()
            .set_columns(2)
            .set_timestamp(datetime(2021, 12, 31, tzinfo=timezone.utc)),
        )


class TestPartition(unittest.TestCase):
    def test_partition_timestamps(self):
        self.assertFalse(PositionPartition("p_start").has_real_time)
        self.assertEqual(
            PositionPartition("p_start").timestamp(),
            datetime(2021, 1, 1, tzinfo=timezone.utc),
        )
        self.assertFalse(PositionPartition("not_a_date").has_real_time)
        self.assertIsNone(PositionPartition("not_a_date").timestamp())
        self.assertFalse(PositionPartition("p_202012310130").has_real_time)
        self.assertIsNone(PositionPartition("p_202012310130").timestamp())

        self.assertTrue(PositionPartition("p_20011231").has_real_time)
        self.assertEqual(
            PositionPartition("p_20011231").timestamp(),
            datetime(2001, 12, 31, tzinfo=timezone.utc),
        )

        self.assertLess(mkPPart("a", 9), mkPPart("b", 11))
        self.assertLess(mkPPart("a", 10), mkPPart("b", 11))
        self.assertFalse(mkPPart("a", 11) < mkPPart("b", 11))
        self.assertFalse(mkPPart("a", 12) < mkPPart("b", 11))

        self.assertLess(mkPPart("a", 10, 10), mkTailPart("b", count=2))
        with self.assertRaises(UnexpectedPartitionException):
            mkPPart("a", 10, 10) < mkTailPart("b", count=1)

        self.assertTrue(mkPPart("a", 10, 10) < mkPPart("b", 11, 10))
        self.assertTrue(mkPPart("a", 10, 10) < mkPPart("b", 10, 11))
        self.assertLess(mkPPart("a", 10, 10), mkPPart("b", 11, 11))
        self.assertTrue(mkPPart("a", 10, 10) < [10, 11])
        self.assertTrue(mkPPart("a", 10, 10) < [11, 10])
        self.assertLess(mkPPart("a", 10, 10), [11, 11])

        with self.assertRaises(UnexpectedPartitionException):
            mkPPart("a", 10, 10) < mkPPart("b", 11, 11, 11)
        with self.assertRaises(UnexpectedPartitionException):
            mkPPart("a", 10, 10, 10) < mkPPart("b", 11, 11)

    def test_partition_tuple_ordering(self):
        cur_pos = mkPPart("current_pos", 8236476764, 6096376984)
        p_20220525 = mkPPart("p_20220525", 2805308158, 2682458996)
        p_20220611 = mkPPart("p_20220611", 7882495694, 7856340600)
        p_20230519 = mkPPart("p_20230519", 10790547177, 11048018089)
        p_20230724 = mkPPart("p_20230724", 95233456870, 97348306298)

        self.assertGreater(cur_pos, p_20220525)
        self.assertGreater(cur_pos, p_20220611)
        self.assertLess(cur_pos, p_20230519)
        self.assertLess(cur_pos, p_20230724)

    def test_instant_partition(self):
        now = datetime.now(tz=timezone.utc)

        ip = InstantPartition("p_20380101", now, [1, 2])
        self.assertEqual(ip.position.as_list(), [1, 2])
        self.assertEqual(ip.name, "p_20380101")
        self.assertEqual(ip.timestamp(), now)

    def test_is_partition_type(self):
        self.assertTrue(is_partition_type(mkPPart("b", 1, 2)))
        self.assertTrue(
            is_partition_type(
                InstantPartition("p_19490520", datetime.now(tz=timezone.utc), [1, 2])
            )
        )
        self.assertFalse(is_partition_type(None))
        self.assertFalse(is_partition_type(1))
        self.assertFalse(is_partition_type(NewPlannedPartition()))


class TestPosition(unittest.TestCase):
    def test_position_as_sql_input(self):
        self.assertEqual([SqlInput(88)], mkPos(88).as_sql_input())
        self.assertEqual([SqlInput(88), SqlInput(99)], mkPos(88, 99).as_sql_input())

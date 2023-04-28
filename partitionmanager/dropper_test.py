import unittest
from datetime import datetime, timedelta, timezone

from .dropper import _drop_statement, get_droppable_partitions
from .types import (
    DatabaseCommand,
    Table,
    SqlInput,
    SqlQuery,
    PositionPartition,
)
from .types_test import mkPPart, mkTailPart, mkPos


def _timestamp_rsp(year, mo, day):
    return [
        {"UNIX_TIMESTAMP": datetime(year, mo, day, tzinfo=timezone.utc).timestamp()}
    ]


class MockDatabase(DatabaseCommand):
    def __init__(self):
        self._responses = list()
        self.num_queries = 0

    def add_response(self, expected, response):
        self._responses.insert(0, {"expected": expected, "response": response})

    def run(self, cmd):
        self.num_queries += 1
        if not self._responses:
            raise Exception(f"No mock responses available for cmd [{cmd}]")

        r = self._responses.pop()
        if r["expected"] in cmd:
            return r["response"]

        raise Exception(f"Received command [{cmd}] and expected [{r['expected']}]")

    def db_name(self):
        return SqlInput("the-database")


class TestDropper(unittest.TestCase):
    def test_drop_statement_empty(self):
        table = Table("burgers")
        parts = []
        with self.assertRaises(ValueError):
            _drop_statement(table, parts)

    def test_drop_statement(self):
        table = Table("burgers")
        parts = [PositionPartition("p_start")]
        self.assertEqual(
            _drop_statement(table, parts),
            "ALTER TABLE `burgers` DROP PARTITION IF EXISTS `p_start`;",
        )

    def test_get_droppable_partitions_invalid_config(self):
        database = MockDatabase()
        table = Table("burgers")
        partitions = [PositionPartition("p_start")]
        current_timestamp = datetime(2021, 1, 1, tzinfo=timezone.utc)
        current_position = PositionPartition("p_20210102").set_position([10])

        with self.assertRaises(ValueError):
            get_droppable_partitions(
                database, partitions, current_position, current_timestamp, table
            )

    def test_get_droppable_partitions(self):
        database = MockDatabase()
        database.add_response("WHERE `id` > '100'", _timestamp_rsp(2021, 5, 20))
        database.add_response("WHERE `id` > '200'", _timestamp_rsp(2021, 5, 27))
        database.add_response("WHERE `id` > '200'", _timestamp_rsp(2021, 5, 27))
        database.add_response("WHERE `id` > '300'", _timestamp_rsp(2021, 6, 3))
        database.add_response("WHERE `id` > '300'", _timestamp_rsp(2021, 6, 3))
        database.add_response("WHERE `id` > '400'", _timestamp_rsp(2021, 6, 10))
        database.add_response("WHERE `id` > '400'", _timestamp_rsp(2021, 6, 10))
        database.add_response("WHERE `id` > '500'", _timestamp_rsp(2021, 6, 17))

        table = Table("burgers")
        table.set_earliest_utc_timestamp_query(
            SqlQuery(
                "SELECT UNIX_TIMESTAMP(`cooked`) FROM `orders` "
                "WHERE `id` > '?' ORDER BY `id` ASC LIMIT 1;"
            )
        )
        current_timestamp = datetime(2021, 7, 1, tzinfo=timezone.utc)

        partitions = [
            mkPPart("1", 100),
            mkPPart("2", 200),
            mkPPart("3", 300),
            mkPPart("4", 400),
            mkPPart("5", 500),
            mkPPart("6", 600),
            mkTailPart("z"),
        ]
        current_position = mkPos(340)

        table.set_retention_period(timedelta(days=2))
        results = get_droppable_partitions(
            database, partitions, current_position, current_timestamp, table
        )
        self.assertEqual(
            results["drop_query"],
            "ALTER TABLE `burgers` DROP PARTITION IF EXISTS `1`,`2`;",
        )
        self.assertEqual(results["1"]["oldest_time"], "2021-05-20 00:00:00+00:00")
        self.assertEqual(results["1"]["youngest_time"], "2021-05-27 00:00:00+00:00")
        self.assertEqual(results["1"]["oldest_position"].as_list(), [100])
        self.assertEqual(results["1"]["youngest_position"].as_list(), [200])
        self.assertEqual(results["1"]["oldest_age"], "42 days, 0:00:00")
        self.assertEqual(results["1"]["youngest_age"], "35 days, 0:00:00")
        self.assertEqual(results["1"]["approx_size"], 100)

        self.assertEqual(results["2"]["oldest_time"], "2021-05-27 00:00:00+00:00")
        self.assertEqual(results["2"]["youngest_time"], "2021-06-03 00:00:00+00:00")
        self.assertEqual(results["2"]["oldest_position"].as_list(), [200])
        self.assertEqual(results["2"]["youngest_position"].as_list(), [300])
        self.assertEqual(results["2"]["oldest_age"], "35 days, 0:00:00")
        self.assertEqual(results["2"]["youngest_age"], "28 days, 0:00:00")
        self.assertEqual(results["2"]["approx_size"], 100)

    def test_drop_nothing_to_do(self):
        database = MockDatabase()
        database.add_response("WHERE `id` > '100'", _timestamp_rsp(2021, 5, 1))
        database.add_response("WHERE `id` > '200'", _timestamp_rsp(2021, 5, 8))
        database.add_response("WHERE `id` > '200'", _timestamp_rsp(2021, 5, 8))
        database.add_response("WHERE `id` > '300'", _timestamp_rsp(2021, 5, 19))
        database.add_response("WHERE `id` > '300'", _timestamp_rsp(2021, 5, 19))
        database.add_response("WHERE `id` > '400'", _timestamp_rsp(2021, 5, 24))

        table = Table("burgers")
        table.set_earliest_utc_timestamp_query(
            SqlQuery(
                "SELECT UNIX_TIMESTAMP(`cooked`) FROM `orders` "
                "WHERE `id` > '?' ORDER BY `id` ASC LIMIT 1;"
            )
        )
        current_timestamp = datetime(2021, 6, 1, tzinfo=timezone.utc)

        partitions = [
            mkPPart("1", 100),
            mkPPart("2", 200),
            mkPPart("3", 300),
            mkPPart("4", 400),
            mkPPart("5", 500),
            mkPPart("6", 600),
            mkTailPart("z"),
        ]
        current_position = mkPos(340)

        table.set_retention_period(timedelta(days=30))
        results = get_droppable_partitions(
            database, partitions, current_position, current_timestamp, table
        )
        self.assertNotIn("drop_query", results)

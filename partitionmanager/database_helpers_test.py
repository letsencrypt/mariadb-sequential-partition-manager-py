import unittest

from .database_helpers import get_position_of_table, calculate_exact_timestamp_via_query

from .types import (
    DatabaseCommand,
    NoExactTimeException,
    PositionPartition,
    SqlInput,
    SqlQuery,
    Table,
    TableEmptyException,
)


class MockDatabase(DatabaseCommand):
    def __init__(self):
        self._responses = []
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


class TestDatabaseHelpers(unittest.TestCase):
    def test_position_of_table(self):
        db = MockDatabase()
        db.add_response("SELECT id FROM `burgers` ORDER BY", [{"id": 90210}])

        table = Table("burgers")
        data = {"range_cols": ["id"]}

        pos = get_position_of_table(db, table, data)
        self.assertEqual(pos.as_list(), [90210])

    def test_empty_table(self):
        db = MockDatabase()
        db.add_response("SELECT id FROM `burgers` ORDER BY", [])

        table = Table("burgers")
        data = {"range_cols": ["id"]}

        with self.assertRaises(TableEmptyException):
            get_position_of_table(db, table, data)

    def test_exact_timestamp_no_query(self):
        db = MockDatabase()
        db.add_response("SELECT id FROM `burgers` ORDER BY", [{"id": 42}])

        table = Table("burgers")
        self.assertFalse(table.has_date_query)

        pos = PositionPartition("p_start")
        pos.set_position([42])

        with self.assertRaises(ValueError):
            calculate_exact_timestamp_via_query(db, table, pos)

    def test_exact_timestamp(self):
        db = MockDatabase()
        db.add_response(
            "SELECT UNIX_TIMESTAMP(`cooked`)", [{"UNIX_TIMESTAMP": 17541339060}]
        )

        table = Table("burgers")
        table.set_earliest_utc_timestamp_query(
            SqlQuery(
                "SELECT UNIX_TIMESTAMP(`cooked`) FROM `orders` "
                "WHERE `type` = \"burger\" AND `id` > '?' ORDER BY `id` ASC LIMIT 1;"
            )
        )

        pos = PositionPartition("p_start")
        pos.set_position([150])

        ts = calculate_exact_timestamp_via_query(db, table, pos)
        assert f"{ts}" == "2525-11-11 18:11:00+00:00"

    def test_no_exact_timestamp(self):
        db = MockDatabase()
        db.add_response(
            "SELECT UNIX_TIMESTAMP(`cooked`)",
            [{"UNIX_TIMESTAMP": 17541339060}, {"UNIX_TIMESTAMP": 17541339070}],
        )

        table = Table("burgers")
        table.set_earliest_utc_timestamp_query(
            SqlQuery(
                "SELECT UNIX_TIMESTAMP(`cooked`) FROM `orders` "
                "WHERE `type` = \"burger\" AND `id` > '?' ORDER BY `id` ASC LIMIT 1;"
            )
        )

        pos = PositionPartition("p_start")
        pos.set_position([150])

        with self.assertRaises(NoExactTimeException):
            calculate_exact_timestamp_via_query(db, table, pos)

        db.add_response(
            "SELECT UNIX_TIMESTAMP(`cooked`)",
            [{"UNIX_TIMESTAMP": 17541339060, "column2": True}],
        )

        with self.assertRaises(NoExactTimeException):
            calculate_exact_timestamp_via_query(db, table, pos)

# flake8: noqa: E501

import unittest
import argparse
from datetime import datetime, timedelta, timezone
from partitionmanager.types import (
    DatabaseCommand,
    DuplicatePartitionException,
    MaxValuePartition,
    MismatchedIdException,
    Partition,
    PositionPartition,
    Table,
    SqlInput,
    TableInformationException,
    UnexpectedPartitionException,
)
from partitionmanager.table_append_partition import (
    get_current_positions,
    get_partition_map,
    assert_table_is_compatible,
    assert_table_information_schema_compatible,
    evaluate_partition_actions,
    parse_partition_map,
    reorganize_partition,
)


class MockDatabase(DatabaseCommand):
    def __init__(self):
        self.response = []

    def run(self, cmd):
        return self.response

    def db_name(self):
        return "the-database"


class TestTypeEnforcement(unittest.TestCase):
    def test_get_partition_map(self):
        with self.assertRaises(ValueError):
            get_partition_map(MockDatabase(), "")

    def test_get_autoincrement(self):
        with self.assertRaises(ValueError):
            assert_table_is_compatible(MockDatabase(), "")


class TestParseTableInformationSchema(unittest.TestCase):
    def test_not_partitioned_and_unexpected(self):
        info = [{"CREATE_OPTIONS": "exfoliated, disenchanted"}]
        with self.assertRaises(TableInformationException):
            assert_table_information_schema_compatible(info, "extable")

    def test_not_partitioned(self):
        info = [{"CREATE_OPTIONS": "exfoliated"}]
        with self.assertRaises(TableInformationException):
            assert_table_information_schema_compatible(info, "extable")

    def test_normal(self):
        info = [{"CREATE_OPTIONS": "partitioned"}]
        assert_table_information_schema_compatible(info, "table")

    def test_normal_multiple_create_options(self):
        info = [{"CREATE_OPTIONS": "magical, partitioned"}]
        assert_table_information_schema_compatible(info, "table")


class TestParsePartitionMap(unittest.TestCase):
    def test_single_partition(self):
        create_stmt = [
            {
                "Table": "dwarves",
                "Create Table": """CREATE TABLE `dwarves` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`),
) ENGINE=InnoDB AUTO_INCREMENT=3101009 DEFAULT CHARSET=utf8
 PARTITION BY RANGE (`id`)
(PARTITION `p_20201204` VALUES LESS THAN MAXVALUE ENGINE = InnoDB)
""",
            }
        ]
        results = parse_partition_map(create_stmt)
        self.assertEqual(len(results["partitions"]), 1)
        self.assertEqual(results["partitions"][0], mkTailPart("p_20201204"))
        self.assertEqual(results["range_cols"], ["id"])

    def test_two_partitions(self):
        create_stmt = [
            {
                "Table": "dwarves",
                "Create Table": """CREATE TABLE `dwarves` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`),
) ENGINE=InnoDB AUTO_INCREMENT=3101009 DEFAULT CHARSET=utf8
 PARTITION BY RANGE (`id`)
(PARTITION `before` VALUES LESS THAN (100),
PARTITION `p_20201204` VALUES LESS THAN MAXVALUE ENGINE = InnoDB)
""",
            }
        ]
        results = parse_partition_map(create_stmt)
        self.assertEqual(len(results["partitions"]), 2)
        self.assertEqual(results["partitions"][0], mkPPart("before", 100))
        self.assertEqual(results["partitions"][1], mkTailPart("p_20201204"))
        self.assertEqual(results["range_cols"], ["id"])

    def test_dual_keys_single_partition(self):
        create_stmt = [
            {
                "Table": "doubleKey",
                "Create Table": """CREATE TABLE `doubleKey` (
                `firstID` bigint(20) NOT NULL,
                `secondID` bigint(20) NOT NULL,
                PRIMARY KEY (`firstID`,`secondID`),
              ) ENGINE=InnoDB DEFAULT CHARSET=utf8
              PARTITION BY RANGE (`firstID`, `secondID`)
              (PARTITION `p_start` VALUES LESS THAN (MAXVALUE, MAXVALUE) ENGINE = InnoDB)""",
            }
        ]
        results = parse_partition_map(create_stmt)
        self.assertEqual(len(results["partitions"]), 1)
        self.assertEqual(results["partitions"][0], mkTailPart("p_start", count=2))
        self.assertEqual(results["range_cols"], ["firstID", "secondID"])

    def test_dual_keys_multiple_partitions(self):
        create_stmt = [
            {
                "Table": "doubleKey",
                "Create Table": """CREATE TABLE `doubleKey` (
                `firstID` bigint(20) NOT NULL,
                `secondID` bigint(20) NOT NULL,
                PRIMARY KEY (`firstID`,`secondID`),
              ) ENGINE=InnoDB DEFAULT CHARSET=utf8
              PARTITION BY RANGE (`firstID`, `secondID`)
              (PARTITION `p_start` VALUES LESS THAN (255, 1234567890),
               PARTITION `p_next` VALUES LESS THAN (MAXVALUE, MAXVALUE) ENGINE = InnoDB)""",
            }
        ]
        results = parse_partition_map(create_stmt)
        self.assertEqual(len(results["partitions"]), 2)
        self.assertEqual(results["partitions"][0], mkPPart("p_start", 255, 1234567890))
        self.assertEqual(results["partitions"][1], mkTailPart("p_next", count=2))
        self.assertEqual(results["range_cols"], ["firstID", "secondID"])


class TestEvaluateShouldPartition(unittest.TestCase):
    def test_partition_without_datestamp(self):
        create_stmt = [
            {
                "Table": "doubleKey",
                "Create Table": """CREATE TABLE `doubleKey` (
                `firstID` bigint(20) NOT NULL,
                `secondID` bigint(20) NOT NULL,
                PRIMARY KEY (`firstID`,`secondID`),
              ) ENGINE=InnoDB DEFAULT CHARSET=utf8
              PARTITION BY RANGE (`firstID`, `secondID`)
              (PARTITION `p_start` VALUES LESS THAN (255, 1234567890),
               PARTITION `p_next` VALUES LESS THAN (MAXVALUE, MAXVALUE) ENGINE = InnoDB)""",
            }
        ]
        results = parse_partition_map(create_stmt)
        decision = evaluate_partition_actions(
            results["partitions"], datetime.utcnow(), timedelta(days=1)
        )
        self.assertTrue(decision["do_partition"])
        self.assertEqual(decision["remaining_lifespan"], timedelta())

    def test_partition_with_datestamp(self):
        create_stmt = [
            {
                "Table": "apples",
                "Create Table": """CREATE TABLE `apples` (
                `id` bigint(20) NOT NULL,
                PRIMARY KEY (`id`),
              ) ENGINE=InnoDB DEFAULT CHARSET=utf8
              PARTITION BY RANGE (`id`)
              (PARTITION `p_20201204` VALUES LESS THAN MAXVALUE ENGINE = InnoDB)""",
            }
        ]
        results = parse_partition_map(create_stmt)

        decision = evaluate_partition_actions(
            results["partitions"],
            datetime(2020, 12, 10, tzinfo=timezone.utc),
            timedelta(days=7),
        )
        self.assertFalse(decision["do_partition"])

        decision = evaluate_partition_actions(
            results["partitions"],
            datetime(2020, 12, 11, tzinfo=timezone.utc),
            timedelta(days=7),
        )
        self.assertTrue(decision["do_partition"])

        decision = evaluate_partition_actions(
            results["partitions"],
            datetime(2020, 12, 12, tzinfo=timezone.utc),
            timedelta(days=7),
        )
        self.assertTrue(decision["do_partition"])

        for i in range(6, 1):
            decision = evaluate_partition_actions(
                results["partitions"],
                datetime(2020, 12, 10, tzinfo=timezone.utc),
                timedelta(days=i),
            )
            self.assertFalse(decision["do_partition"])
            self.assertGreater(decision["remaining_lifespan"], timedelta())

        decision = evaluate_partition_actions(
            results["partitions"],
            datetime(2020, 12, 10, tzinfo=timezone.utc),
            timedelta(days=1),
        )
        self.assertTrue(decision["do_partition"])
        self.assertLess(decision["remaining_lifespan"], timedelta())


class TestSqlInput(unittest.TestCase):
    def test_escaping(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            SqlInput("little bobby `;drop tables;")

    def test_whitespace(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            SqlInput("my table")

    def test_okay(self):
        SqlInput("my_table")
        SqlInput("zz-table")


def mkPPart(name, *pos):
    p = PositionPartition(name)
    for x in pos:
        p.add_position(x)
    return p


def mkTailPart(name, count=1):
    return MaxValuePartition(name, count)


class TestReorganizePartitions(unittest.TestCase):
    def test_list_without_final_entry(self):
        with self.assertRaises(UnexpectedPartitionException):
            reorganize_partition([mkPPart("a", 1), mkPPart("b", 2)], "new", [3])

    def test_reorganize_with_duplicate(self):
        with self.assertRaises(DuplicatePartitionException):
            reorganize_partition([mkPPart("a", 1), mkTailPart("b")], "b", [3])

    def test_reorganize_single_partition(self):
        last_value, reorg_list = reorganize_partition([mkTailPart("a")], "b", [1])
        self.assertEqual(last_value, "a")
        self.assertEqual(reorg_list, [mkPPart("a", 1), mkTailPart("b")])

    def test_reorganize(self):
        last_value, reorg_list = reorganize_partition(
            [mkPPart("a", 1), mkTailPart("b")], "c", [2]
        )
        self.assertEqual(last_value, "b")
        self.assertEqual(reorg_list, [mkPPart("b", 2), mkTailPart("c")])

    def test_reorganize_too_many_partition_ids(self):
        with self.assertRaises(MismatchedIdException):
            reorganize_partition([mkPPart("a", 1), mkTailPart("b")], "c", [2, 3, 4])

    def test_reorganize_too_few_partition_ids(self):
        with self.assertRaises(MismatchedIdException):
            reorganize_partition([mkPPart("a", 1, 1, 1), mkTailPart("b")], "c", [2, 3])

    def test_reorganize_with_dual_keys(self):
        last_value, reorg_list = reorganize_partition(
            [mkPPart("p_start", 255, 1234567890), mkTailPart("p_next", count=2)],
            "new",
            [512, 2345678901],
        )
        self.assertEqual(last_value, "p_next")
        self.assertEqual(
            reorg_list, [mkPPart("p_next", 512, 2345678901), mkTailPart("new", count=2)]
        )


class TestGetPositions(unittest.TestCase):
    def test_get_position_single_column_wrong_type(self):
        db = MockDatabase()
        db.response = [{"id": 0}]

        with self.assertRaises(ValueError):
            get_current_positions(db, Table("table"), "id")

    def test_get_position_single_column(self):
        db = MockDatabase()
        db.response = [{"id": 1}]

        p = get_current_positions(db, Table("table"), ["id"])
        self.assertEqual(len(p), 1)
        self.assertEqual(p[0], 1)

    def test_get_position_two_columns(self):
        db = MockDatabase()
        db.response = [{"id": 1, "id2": 2}]

        p = get_current_positions(db, Table("table"), ["id", "id2"])
        self.assertEqual(len(p), 2)
        self.assertEqual(p[0], 1)
        self.assertEqual(p[1], 2)


if __name__ == "__main__":
    unittest.main()

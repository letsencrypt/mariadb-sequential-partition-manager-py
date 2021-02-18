# flake8: noqa: E501

import unittest
import argparse
from partitionmanager.types import (
    DatabaseCommand,
    DuplicatePartitionException,
    TableInformationException,
    MismatchedIdException,
    SqlInput,
    UnexpectedPartitionException,
)
from partitionmanager.table_append_partition import (
    parse_table_information_schema,
    parse_partition_map,
    get_autoincrement,
    get_current_positions,
    get_partition_map,
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
            get_autoincrement(MockDatabase(), "")


class TestParseTableInformationSchema(unittest.TestCase):
    def test_null_auto_increment(self):
        info = [{"AUTO_INCREMENT": None, "CREATE_OPTIONS": "partitioned"}]
        with self.assertRaises(TableInformationException):
            parse_table_information_schema(info)

    def test_not_partitioned_and_unexpected(self):
        info = [{"AUTO_INCREMENT": None, "CREATE_OPTIONS": "exfoliated, disenchanted"}]
        with self.assertRaises(TableInformationException):
            parse_table_information_schema(info)

    def test_auto_increment_not_int(self):
        info = [{"AUTO_INCREMENT": 1.21, "CREATE_OPTIONS": "jiggawatts, partitioned"}]
        with self.assertRaises(TableInformationException):
            parse_table_information_schema(info)

    def test_not_partitioned(self):
        info = [{"AUTO_INCREMENT": 2, "CREATE_OPTIONS": "exfoliated"}]
        with self.assertRaises(TableInformationException):
            parse_table_information_schema(info)

    def test_normal(self):
        info = [{"AUTO_INCREMENT": 3101009, "CREATE_OPTIONS": "partitioned"}]
        self.assertEqual(parse_table_information_schema(info), 3101009)

    def test_normal_multiple_create_options(self):
        info = [{"AUTO_INCREMENT": 3101009, "CREATE_OPTIONS": "magical, partitioned"}]
        self.assertEqual(parse_table_information_schema(info), 3101009)


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
        self.assertEqual(results["partitions"][0], "p_20201204")
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
        self.assertEqual(results["partitions"][0], ("before", [100]))
        self.assertEqual(results["partitions"][1], "p_20201204")
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
        self.assertEqual(results["partitions"][0], "p_start")
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
        self.assertEqual(results["partitions"][0], ("p_start", [255, 1234567890]))
        self.assertEqual(results["partitions"][1], "p_next")
        self.assertEqual(results["range_cols"], ["firstID", "secondID"])


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


class TestReorganizePartitions(unittest.TestCase):
    def test_list_without_final_entry(self):
        with self.assertRaises(UnexpectedPartitionException):
            reorganize_partition([("a", 1), ("b", 2)], "new", [3])

    def test_reorganize_with_duplicate(self):
        with self.assertRaises(DuplicatePartitionException):
            reorganize_partition([("a", 1), "b"], "b", [3])

    def test_reorganize(self):
        last_value, reorg_list = reorganize_partition([("a", 1), "b"], "c", [2])
        self.assertEqual(last_value, "b")
        self.assertEqual(reorg_list, [("b", "(2)"), ("c", "MAXVALUE")])

    def test_reorganize_too_many_partition_ids(self):
        with self.assertRaises(MismatchedIdException):
            reorganize_partition([("a", 1), "b"], "c", [2, 3, 4])

    def test_reorganize_too_few_partition_ids(self):
        with self.assertRaises(MismatchedIdException):
            reorganize_partition([("a", [1, 1, 1]), "b"], "c", [2, 3])

    def test_reorganize_with_dual_keys(self):
        last_value, reorg_list = reorganize_partition(
            [("p_start", [255, 1234567890]), "p_next"], "new", [512, 2345678901]
        )
        self.assertEqual(last_value, "p_next")
        self.assertEqual(
            reorg_list, [("p_next", "(512, 2345678901)"), ("new", "MAXVALUE, MAXVALUE")]
        )


class TestGetPositions(unittest.TestCase):
    def test_get_position_single_column_wrong_type(self):
        db = MockDatabase()
        db.response = [{"id": 0}]

        with self.assertRaises(ValueError):
            get_current_positions(db, "table", "id")

    def test_get_position_single_column(self):
        db = MockDatabase()
        db.response = [{"id": 1}]

        p = get_current_positions(db, "table", ["id"])
        self.assertEqual(len(p), 1)
        self.assertEqual(p[0], 1)

    def test_get_position_two_columns(self):
        db = MockDatabase()
        db.response = [{"id": 1, "id2": 2}]

        p = get_current_positions(db, "table", ["id", "id2"])
        self.assertEqual(len(p), 2)
        self.assertEqual(p[0], 1)
        self.assertEqual(p[1], 2)


if __name__ == "__main__":
    unittest.main()

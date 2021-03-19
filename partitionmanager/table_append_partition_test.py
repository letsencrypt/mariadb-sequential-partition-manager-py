# flake8: noqa: E501

import unittest
import argparse
from datetime import datetime, timedelta, timezone
from partitionmanager.types import (
    ChangedPartition,
    DatabaseCommand,
    DuplicatePartitionException,
    MaxValuePartition,
    MismatchedIdException,
    NewPartition,
    NoEmptyPartitionsAvailableException,
    Partition,
    PositionPartition,
    SqlInput,
    Table,
    TableInformationException,
    UnexpectedPartitionException,
)
from partitionmanager.table_append_partition import (
    evaluate_partition_actions,
    generate_weights,
    get_current_positions,
    get_partition_map,
    get_position_increase_per_day,
    get_weighted_position_increase_per_day_for_partitions,
    parse_partition_map,
    plan_partition_changes,
    predict_forward,
    reorganize_partition,
    split_partitions_around_positions,
    table_information_schema_is_compatible,
    table_is_compatible,
)

from .types_test import mkPPart, mkTailPart


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
        self.assertEqual(
            table_is_compatible(MockDatabase(), ""), "Unexpected table type: "
        )


class TestParseTableInformationSchema(unittest.TestCase):
    def test_not_partitioned_and_unexpected(self):
        info = [{"CREATE_OPTIONS": "exfoliated, disenchanted"}]
        self.assertIsNotNone(table_information_schema_is_compatible(info, "extable"))

    def test_not_partitioned(self):
        info = [{"CREATE_OPTIONS": "exfoliated"}]
        self.assertIsNotNone(table_information_schema_is_compatible(info, "extable"))

    def test_normal(self):
        info = [{"CREATE_OPTIONS": "partitioned"}]
        self.assertIsNone(table_information_schema_is_compatible(info, "table"))

    def test_normal_multiple_create_options(self):
        info = [{"CREATE_OPTIONS": "magical, partitioned"}]
        self.assertIsNone(table_information_schema_is_compatible(info, "table"))


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
              PARTITION BY RANGE COLUMNS(`firstID`, `secondID`)
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
              PARTITION BY RANGE  COLUMNS(`firstID`, `secondID`)
              (PARTITION `p_start` VALUES LESS THAN (255, 1234567890),
               PARTITION `p_next` VALUES LESS THAN (MAXVALUE, MAXVALUE) ENGINE = InnoDB)""",
            }
        ]
        results = parse_partition_map(create_stmt)
        self.assertEqual(len(results["partitions"]), 2)
        self.assertEqual(results["partitions"][0], mkPPart("p_start", 255, 1234567890))
        self.assertEqual(results["partitions"][1], mkTailPart("p_next", count=2))
        self.assertEqual(results["range_cols"], ["firstID", "secondID"])

    def test_missing_part_definition(self):
        create_stmt = [
            {
                "Table": "doubleKey",
                "Create Table": """CREATE TABLE `doubleKey` (
                `firstID` bigint(20) NOT NULL,
                `secondID` bigint(20) NOT NULL,
                PRIMARY KEY (`firstID`,`secondID`),
              ) ENGINE=InnoDB DEFAULT CHARSET=utf8
              (PARTITION `p_start` VALUES LESS THAN (255, 1234567890),
               PARTITION `p_next` VALUES LESS THAN (MAXVALUE, MAXVALUE) ENGINE = InnoDB)""",
            }
        ]
        with self.assertRaises(TableInformationException):
            parse_partition_map(create_stmt)

    def test_missing_part_definition_and_just_tail(self):
        create_stmt = [
            {
                "Table": "doubleKey",
                "Create Table": """CREATE TABLE `doubleKey` (
                `firstID` bigint(20) NOT NULL,
                `secondID` bigint(20) NOT NULL,
                PRIMARY KEY (`firstID`,`secondID`),
              ) ENGINE=InnoDB DEFAULT CHARSET=utf8
              (PARTITION `p_next` VALUES LESS THAN (MAXVALUE, MAXVALUE) ENGINE = InnoDB)""",
            }
        ]
        with self.assertRaises(TableInformationException):
            parse_partition_map(create_stmt)

    def test_missing_part_tail(self):
        create_stmt = [
            {
                "Table": "doubleKey",
                "Create Table": """CREATE TABLE `doubleKey` (
                `firstID` bigint(20) NOT NULL,
                `secondID` bigint(20) NOT NULL,
                PRIMARY KEY (`firstID`,`secondID`),
              ) ENGINE=InnoDB DEFAULT CHARSET=utf8
              PARTITION BY RANGE  COLUMNS(`firstID`, `secondID`)""",
            }
        ]
        with self.assertRaises(UnexpectedPartitionException):
            parse_partition_map(create_stmt)


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
              PARTITION BY RANGE  COLUMNS(`firstID`, `secondID`)
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


class TestPartitionAlgorithm(unittest.TestCase):
    def test_split(self):
        with self.assertRaises(UnexpectedPartitionException):
            split_partitions_around_positions(
                [mkPPart("a", 1), mkTailPart("z")], [10, 10]
            )
        with self.assertRaises(UnexpectedPartitionException):
            split_partitions_around_positions(
                [mkPPart("a", 1, 1), mkTailPart("z")], [10, 10]
            )
        with self.assertRaises(UnexpectedPartitionException):
            split_partitions_around_positions(
                [mkPPart("a", 1), mkTailPart("z", count=2)], [10, 10]
            )

        self.assertEqual(
            split_partitions_around_positions(
                [mkPPart("a", 1), mkPPart("b", 2), mkTailPart("z")], [10]
            ),
            ([mkPPart("a", 1), mkPPart("b", 2)], mkTailPart("z"), []),
        )

        self.assertEqual(
            split_partitions_around_positions(
                [mkPPart("a", 100), mkPPart("b", 200), mkTailPart("z")], [10]
            ),
            ([], mkPPart("a", 100), [mkPPart("b", 200), mkTailPart("z")]),
        )

        self.assertEqual(
            split_partitions_around_positions(
                [mkPPart("a", 1), mkPPart("b", 10), mkTailPart("z")], [10]
            ),
            ([mkPPart("a", 1)], mkPPart("b", 10), [mkTailPart("z")]),
        )
        self.assertEqual(
            split_partitions_around_positions(
                [mkPPart("a", 1), mkPPart("b", 11), mkTailPart("z")], [10]
            ),
            ([mkPPart("a", 1)], mkPPart("b", 11), [mkTailPart("z")]),
        )

        self.assertEqual(
            split_partitions_around_positions(
                [mkPPart("a", 1), mkPPart("b", 11), mkPPart("c", 11), mkTailPart("z")],
                [10],
            ),
            ([mkPPart("a", 1)], mkPPart("b", 11), [mkPPart("c", 11), mkTailPart("z")]),
        )

        self.assertEqual(
            split_partitions_around_positions(
                [mkPPart("a", 1), mkPPart("b", 11), mkPPart("c", 11), mkTailPart("z")],
                [0],
            ),
            (
                [],
                mkPPart("a", 1),
                [mkPPart("b", 11), mkPPart("c", 11), mkTailPart("z")],
            ),
        )

        self.assertEqual(
            split_partitions_around_positions(
                [mkPPart("a", 1), mkPPart("b", 11), mkPPart("c", 11), mkTailPart("z")],
                [200],
            ),
            (
                [mkPPart("a", 1), mkPPart("b", 11), mkPPart("c", 11)],
                mkTailPart("z"),
                [],
            ),
        )

        self.assertEqual(
            split_partitions_around_positions(
                [mkPPart("a", 1, 100), mkPPart("b", 2, 200), mkTailPart("z", count=2)],
                [10, 1000],
            ),
            (
                [mkPPart("a", 1, 100), mkPPart("b", 2, 200)],
                mkTailPart("z", count=2),
                [],
            ),
        )

        self.assertEqual(
            split_partitions_around_positions(
                [mkPPart("a", 10, 10), mkPPart("b", 20, 20), mkTailPart("z", count=2)],
                [19, 500],
            ),
            ([mkPPart("a", 10, 10)], mkPPart("b", 20, 20), [mkTailPart("z", count=2)]),
        )

    def test_get_position_increase_per_day(self):
        with self.assertRaises(ValueError):
            get_position_increase_per_day(
                mkTailPart("p_20201231"), mkPPart("p_20210101", 42)
            )
        with self.assertRaises(ValueError):
            get_position_increase_per_day(
                mkPPart("p_20211231", 99), mkPPart("p_20210101", 42)
            )
        with self.assertRaises(ValueError):
            get_position_increase_per_day(
                mkPPart("p_20201231", 1, 99), mkPPart("p_20210101", 42)
            )

        self.assertEqual(
            get_position_increase_per_day(
                mkPPart("p_20201231", 0), mkPPart("p_20210101", 100)
            ),
            [100],
        )
        self.assertEqual(
            get_position_increase_per_day(
                mkPPart("p_20201231", 0), mkPPart("p_20210410", 100)
            ),
            [1],
        )
        self.assertEqual(
            get_position_increase_per_day(
                mkPPart("p_20201231", 0, 10), mkPPart("p_20210410", 100, 1000)
            ),
            [1, 9.9],
        )

    def test_generate_weights(self):
        self.assertEqual(generate_weights(1), [10000])
        self.assertEqual(generate_weights(3), [10000 / 3, 5000, 10000])

    def test_get_weighted_position_increase_per_day_for_partitions(self):
        with self.assertRaises(ValueError):
            get_weighted_position_increase_per_day_for_partitions(list())

        self.assertEqual(
            get_weighted_position_increase_per_day_for_partitions(
                [mkPPart("p_20201231", 0), mkPPart("p_20210101", 100)]
            ),
            [100],
        )
        self.assertEqual(
            get_weighted_position_increase_per_day_for_partitions(
                [mkPPart("p_20201231", 0), mkPPart("p_20210410", 100)]
            ),
            [1],
        )
        self.assertEqual(
            get_weighted_position_increase_per_day_for_partitions(
                [mkPPart("p_20201231", 50, 50), mkPPart("p_20210410", 100, 500)]
            ),
            [0.5, 4.5],
        )
        self.assertEqual(
            get_weighted_position_increase_per_day_for_partitions(
                [
                    mkPPart("p_20200922", 0),
                    mkPPart("p_20201231", 100),  # rate = 1/day
                    mkPPart("p_20210410", 1100),  # rate = 10/day
                ]
            ),
            [7],
        )
        self.assertEqual(
            get_weighted_position_increase_per_day_for_partitions(
                [
                    mkPPart("p_20200922", 0),
                    mkPPart("p_20201231", 100),  # 1/day
                    mkPPart("p_20210410", 1100),  # 10/day
                    mkPPart("p_20210719", 101100),  # 1,000/day
                ]
            ),
            [548.3636363636364],
        )

    def test_predict_forward(self):
        with self.assertRaises(ValueError):
            predict_forward([0], [1, 2], timedelta(days=1))
        with self.assertRaises(ValueError):
            predict_forward([1, 2], [3], timedelta(days=1))
        with self.assertRaises(ValueError):
            predict_forward([1, 2], [-1], timedelta(days=1))

        self.assertEqual(predict_forward([0], [500], timedelta(days=1)), [500])

        self.assertEqual(predict_forward([0], [125], timedelta(days=4)), [500])

    def test_plan_partition_changes_no_empty_partitions(self):
        with self.assertRaises(NoEmptyPartitionsAvailableException):
            plan_partition_changes(
                [mkPPart("p_20201231", 0), mkPPart("p_20210102", 200)],
                [50],
                datetime(2021, 1, 1, tzinfo=timezone.utc),
                timedelta(days=7),
                2,
            )

    def test_plan_partition_changes(self):
        self.assertEqual(
            plan_partition_changes(
                [
                    mkPPart("p_20201231", 100),
                    mkPPart("p_20210102", 200),
                    mkTailPart("future"),
                ],
                [50],
                datetime(2021, 1, 1, tzinfo=timezone.utc),
                timedelta(days=7),
                2,
            ),
            [
                ChangedPartition(mkPPart("p_20201231", 100)).set_position([350.0]),
                ChangedPartition(mkPPart("p_20210102", 200))
                .set_position([700.0])
                .set_timestamp(datetime(2021, 1, 7, tzinfo=timezone.utc)),
                ChangedPartition(mkTailPart("future"))
                .set_position([1050.0])
                .set_timestamp(datetime(2021, 1, 14, tzinfo=timezone.utc)),
            ],
        )


if __name__ == "__main__":
    unittest.main()

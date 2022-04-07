# flake8: noqa: E501

import unittest
import argparse
from datetime import datetime, timedelta, timezone
from partitionmanager.types import (
    ChangePlannedPartition,
    DatabaseCommand,
    DuplicatePartitionException,
    MaxValuePartition,
    MismatchedIdException,
    NewPlannedPartition,
    NoEmptyPartitionsAvailableException,
    PositionPartition,
    InstantPartition,
    SqlInput,
    SqlQuery,
    Table,
    TableInformationException,
    UnexpectedPartitionException,
)
from partitionmanager.table_append_partition import (
    _generate_weights,
    _get_position_increase_per_day,
    _get_rate_partitions_with_implicit_timestamps,
    _get_rate_partitions_with_queried_timestamps,
    _get_table_information_schema_problems,
    _get_weighted_position_increase_per_day_for_partitions,
    _parse_columns,
    _parse_partition_map,
    _plan_partition_changes,
    _predict_forward_position,
    _predict_forward_time,
    _should_run_changes,
    _split_partitions_around_position,
    generate_sql_reorganize_partition_commands,
    get_current_positions,
    get_partition_map,
    get_pending_sql_reorganize_partition_commands,
    get_table_compatibility_problems,
    get_columns,
)

from .types_test import mkPPart, mkTailPart, mkPos


class MockDatabase(DatabaseCommand):
    def __init__(self):
        self._response = []
        self._num_queries = 0

    def run(self, cmd):
        self._num_queries += 1
        return self._response.pop()

    def push_response(self, r):
        self._response.append(r)

    @property
    def num_queries(self):
        return self._num_queries

    def db_name(self):
        return "the-database"


class TestTypeEnforcement(unittest.TestCase):
    def test_get_partition_map(self):
        with self.assertRaises(ValueError):
            get_partition_map(MockDatabase(), "")

    def test_get_autoincrement(self):
        self.assertEqual(
            get_table_compatibility_problems(MockDatabase(), ""),
            ["Unexpected table type: "],
        )


class TestParseTableInformationSchema(unittest.TestCase):
    def test_not_partitioned_and_unexpected(self):
        info = [{"CREATE_OPTIONS": "exfoliated, disenchanted"}]
        self.assertEqual(
            _get_table_information_schema_problems(info, "extable"),
            ["Table extable is not partitioned"],
        )

    def test_not_partitioned(self):
        info = [{"CREATE_OPTIONS": "exfoliated"}]
        self.assertEqual(
            _get_table_information_schema_problems(info, "extable"),
            ["Table extable is not partitioned"],
        )

    def test_normal(self):
        info = [{"CREATE_OPTIONS": "partitioned"}]
        self.assertEqual(_get_table_information_schema_problems(info, "table"), list())

    def test_normal_multiple_create_options(self):
        info = [{"CREATE_OPTIONS": "magical, partitioned"}]
        self.assertEqual(_get_table_information_schema_problems(info, "table"), list())


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
        results = _parse_partition_map(create_stmt)
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
        results = _parse_partition_map(create_stmt)
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
        results = _parse_partition_map(create_stmt)
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
        results = _parse_partition_map(create_stmt)
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
            _parse_partition_map(create_stmt)

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
            _parse_partition_map(create_stmt)

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
            _parse_partition_map(create_stmt)


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


class TestGetPositions(unittest.TestCase):
    def test_get_position_single_column_wrong_type(self):
        db = MockDatabase()
        db.push_response([{"id": 0}])

        with self.assertRaises(ValueError):
            get_current_positions(db, Table("table"), "id")

    def test_get_position_single_column(self):
        db = MockDatabase()
        db.push_response([{"id": 1}])

        p = get_current_positions(db, Table("table"), ["id"])
        self.assertEqual(len(p), 1)
        self.assertEqual(p["id"], 1)
        self.assertEqual(db.num_queries, 1)

    def test_get_position_two_columns(self):
        db = MockDatabase()
        db.push_response([{"id": 1, "id2": 2}])
        db.push_response([{"id": 1, "id2": 2}])

        p = get_current_positions(db, Table("table"), ["id", "id2"])
        self.assertEqual(len(p), 2)
        self.assertEqual(p["id"], 1)
        self.assertEqual(p["id2"], 2)
        self.assertEqual(db.num_queries, 2)


class TestPartitionAlgorithm(unittest.TestCase):
    def test_split(self):
        with self.assertRaises(UnexpectedPartitionException):
            _split_partitions_around_position(
                [mkPPart("a", 1), mkTailPart("z")], mkPos(10, 10)
            )
        with self.assertRaises(UnexpectedPartitionException):
            _split_partitions_around_position(
                [mkPPart("a", 1, 1), mkTailPart("z")], mkPos(10, 10)
            )
        with self.assertRaises(UnexpectedPartitionException):
            _split_partitions_around_position(
                [mkPPart("a", 1), mkTailPart("z", count=2)], mkPos(10, 10)
            )

        self.assertEqual(
            _split_partitions_around_position(
                [mkPPart("a", 1), mkPPart("b", 2), mkTailPart("z")], mkPos(10)
            ),
            ([mkPPart("a", 1), mkPPart("b", 2)], mkTailPart("z"), []),
        )

        self.assertEqual(
            _split_partitions_around_position(
                [mkPPart("a", 100), mkPPart("b", 200), mkTailPart("z")], mkPos(10)
            ),
            ([], mkPPart("a", 100), [mkPPart("b", 200), mkTailPart("z")]),
        )

        self.assertEqual(
            _split_partitions_around_position(
                [mkPPart("a", 1), mkPPart("b", 10), mkTailPart("z")], mkPos(10)
            ),
            ([mkPPart("a", 1)], mkPPart("b", 10), [mkTailPart("z")]),
        )
        self.assertEqual(
            _split_partitions_around_position(
                [mkPPart("a", 1), mkPPart("b", 11), mkTailPart("z")], mkPos(10)
            ),
            ([mkPPart("a", 1)], mkPPart("b", 11), [mkTailPart("z")]),
        )

        self.assertEqual(
            _split_partitions_around_position(
                [mkPPart("a", 1), mkPPart("b", 11), mkPPart("c", 11), mkTailPart("z")],
                mkPos(10),
            ),
            ([mkPPart("a", 1)], mkPPart("b", 11), [mkPPart("c", 11), mkTailPart("z")]),
        )

        self.assertEqual(
            _split_partitions_around_position(
                [mkPPart("a", 1), mkPPart("b", 11), mkPPart("c", 11), mkTailPart("z")],
                mkPos(0),
            ),
            (
                [],
                mkPPart("a", 1),
                [mkPPart("b", 11), mkPPart("c", 11), mkTailPart("z")],
            ),
        )

        self.assertEqual(
            _split_partitions_around_position(
                [mkPPart("a", 1), mkPPart("b", 11), mkPPart("c", 11), mkTailPart("z")],
                mkPos(200),
            ),
            (
                [mkPPart("a", 1), mkPPart("b", 11), mkPPart("c", 11)],
                mkTailPart("z"),
                [],
            ),
        )

        self.assertEqual(
            _split_partitions_around_position(
                [mkPPart("a", 1, 100), mkPPart("b", 2, 200), mkTailPart("z", count=2)],
                mkPos(10, 1000),
            ),
            (
                [mkPPart("a", 1, 100), mkPPart("b", 2, 200)],
                mkTailPart("z", count=2),
                [],
            ),
        )

        self.assertEqual(
            _split_partitions_around_position(
                [mkPPart("a", 10, 10), mkPPart("b", 20, 20), mkTailPart("z", count=2)],
                mkPos(19, 500),
            ),
            ([mkPPart("a", 10, 10)], mkPPart("b", 20, 20), [mkTailPart("z", count=2)]),
        )

    def test_get_position_increase_per_day(self):
        with self.assertRaises(ValueError):
            _get_position_increase_per_day(
                mkTailPart("p_20201231"), mkPPart("p_20210101", 42)
            )
        with self.assertRaises(ValueError):
            _get_position_increase_per_day(
                mkPPart("p_20201231", 1, 99), mkPPart("p_20210101", 42)
            )

        self.assertEqual(
            _get_position_increase_per_day(
                mkPPart("p_20211231", 99), mkPPart("p_20210101", 42)
            ),
            [],
        )
        self.assertEqual(
            _get_position_increase_per_day(
                mkPPart("p_20201231", 0), mkPPart("p_20210101", 100)
            ),
            [100],
        )
        self.assertEqual(
            _get_position_increase_per_day(
                mkPPart("p_20201231", 0), mkPPart("p_20210410", 100)
            ),
            [1],
        )
        self.assertEqual(
            _get_position_increase_per_day(
                mkPPart("p_20201231", 0, 10), mkPPart("p_20210410", 100, 1000)
            ),
            [1, 9.9],
        )

    def test_generate_weights(self):
        self.assertEqual(_generate_weights(1), [10000])
        self.assertEqual(_generate_weights(3), [10000 / 3, 5000, 10000])

    def test_get_weighted_position_increase_per_day_for_partitions(self):
        with self.assertRaises(ValueError):
            _get_weighted_position_increase_per_day_for_partitions(list())

        self.assertEqual(
            _get_weighted_position_increase_per_day_for_partitions(
                [mkPPart("p_20201231", 0), mkPPart("p_20210101", 100)]
            ),
            [100],
        )
        self.assertEqual(
            _get_weighted_position_increase_per_day_for_partitions(
                [mkPPart("p_20201231", 0), mkPPart("p_20210410", 100)]
            ),
            [1],
        )
        self.assertEqual(
            _get_weighted_position_increase_per_day_for_partitions(
                [mkPPart("p_20201231", 50, 50), mkPPart("p_20210410", 100, 500)]
            ),
            [0.5, 4.5],
        )
        self.assertEqual(
            _get_weighted_position_increase_per_day_for_partitions(
                [
                    mkPPart("p_20200922", 0),
                    mkPPart("p_20201231", 100),  # rate = 1/day
                    mkPPart("p_20210410", 1100),  # rate = 10/day
                ]
            ),
            [7],
        )
        self.assertEqual(
            _get_weighted_position_increase_per_day_for_partitions(
                [
                    mkPPart("p_20200922", 0),
                    mkPPart("p_20201231", 100),  # 1/day
                    mkPPart("p_20210410", 1100),  # 10/day
                    mkPPart("p_20210719", 101100),  # 1,000/day
                ]
            ),
            [548.3636363636364],
        )

    def test_predict_forward_position(self):
        with self.assertRaises(ValueError):
            _predict_forward_position([0], [1, 2], timedelta(days=1))
        with self.assertRaises(ValueError):
            _predict_forward_position([1, 2], [3], timedelta(days=1))
        with self.assertRaises(ValueError):
            _predict_forward_position([1, 2], [-1], timedelta(days=1))

        self.assertEqual(
            _predict_forward_position([0], [500], timedelta(days=1)), [500]
        )

        self.assertEqual(
            _predict_forward_position([0], [125], timedelta(days=4)), [500]
        )

    def test_predict_forward_time(self):
        t = datetime(2000, 1, 1)

        with self.assertRaises(ValueError):
            _predict_forward_time(mkPos(0, 0), mkPos(100), [100], t)
        with self.assertRaises(ValueError):
            _predict_forward_time(mkPos(0), mkPos(100, 0), [100], t)
        with self.assertRaises(ValueError):
            _predict_forward_time(mkPos(0), mkPos(100, 0), [100, 100], t)
        with self.assertRaises(ValueError):
            _predict_forward_time(mkPos(0), mkPos(100), [100, 100], t)
        with self.assertRaises(ValueError):
            _predict_forward_time(mkPos(0), mkPos(100), [-1], t)
        with self.assertRaises(ValueError):
            _predict_forward_time(mkPos(100), mkPos(99), [1], t)
        with self.assertRaises(ValueError):
            # We should never be asked to operate on positions in the incorrect
            # order
            _predict_forward_time(mkPos(101, 101), mkPos(100, 100), [200, 200], t)
        with self.assertRaises(ValueError):
            # Nonzero rates of change are bad too.
            _predict_forward_time(mkPos(0, 0, 0), mkPos(100, 100, 100), [1, 1, 0], t)

        self.assertEqual(
            _predict_forward_time(mkPos(0), mkPos(100), [100], t),
            t + timedelta(hours=24),
        )
        self.assertEqual(
            _predict_forward_time(mkPos(0), mkPos(100), [200], t),
            t + timedelta(hours=12),
        )
        self.assertEqual(
            _predict_forward_time(mkPos(0), mkPos(100), [200], t),
            t + timedelta(hours=12),
        )

        # It must be OK to have some positions already well beyond the endpoint
        self.assertEqual(
            _predict_forward_time(mkPos(0, 200), mkPos(100, 100), [200, 200], t),
            t + timedelta(hours=12),
        )

        self.assertEqual(
            _predict_forward_time(mkPos(100, 100), mkPos(100, 100), [200, 200], t), t
        )

    def test_plan_partition_changes_no_empty_partitions(self):
        with self.assertRaises(NoEmptyPartitionsAvailableException):
            _plan_partition_changes(
                MockDatabase(),
                Table("table"),
                [mkPPart("p_20201231", 0), mkPPart("p_20210102", 200)],
                mkPos(50),
                datetime(2021, 1, 1, tzinfo=timezone.utc),
                timedelta(days=7),
                2,
            )

    def test_plan_partition_changes_imminent(self):
        with self.assertLogs("plan_partition_changes:table", level="INFO") as logctx:
            planned = _plan_partition_changes(
                MockDatabase(),
                Table("table"),
                [
                    mkPPart("p_20201231", 100),
                    mkPPart("p_20210102", 200),
                    mkTailPart("future"),
                ],
                mkPos(50),
                datetime(2021, 1, 1, hour=23, minute=55, tzinfo=timezone.utc),
                timedelta(days=2),
                3,
            )

        self.assertEqual(
            logctx.output,
            [
                "INFO:plan_partition_changes:table:Rates of change calculated as "
                "[25.043478260869566] per day from 2 partitions",
                "INFO:plan_partition_changes:table:Start-of-fill predicted at "
                "2021-01-03 which is not 2021-01-02. This change will be marked "
                "as important to ensure that p_20210102: (200) is moved to "
                "2021-01-03",
            ],
        )

        self.assertEqual(
            planned,
            [
                ChangePlannedPartition(mkPPart("p_20201231", 100)),
                ChangePlannedPartition(mkPPart("p_20210102", 200))
                .set_timestamp(datetime(2021, 1, 3, tzinfo=timezone.utc))
                .set_important(),
                ChangePlannedPartition(mkTailPart("future"))
                .set_position([250])
                .set_timestamp(datetime(2021, 1, 5, tzinfo=timezone.utc)),
                NewPlannedPartition()
                .set_columns(1)
                .set_timestamp(datetime(2021, 1, 7, tzinfo=timezone.utc)),
            ],
        )

    def test_plan_partition_changes_wildly_off_dates(self):
        with self.assertLogs("plan_partition_changes:table", level="INFO") as logctx:
            planned = _plan_partition_changes(
                MockDatabase(),
                Table("table"),
                [
                    mkPPart("p_20201231", 100),
                    mkPPart("p_20210104", 200),
                    mkTailPart("future"),
                ],
                mkPos(50),
                datetime(2021, 1, 1, tzinfo=timezone.utc),
                timedelta(days=7),
                2,
            )

        self.assertEqual(
            logctx.output,
            [
                "INFO:plan_partition_changes:table:Rates of change calculated as [50.0] per "
                "day from 2 partitions",
                "INFO:plan_partition_changes:table:Start-of-fill predicted at "
                "2021-01-02 which is not 2021-01-04. This change will be marked "
                "as important to ensure that p_20210104: (200) is moved to "
                "2021-01-02",
            ],
        )

        self.assertEqual(
            [
                ChangePlannedPartition(mkPPart("p_20201231", 100)),
                ChangePlannedPartition(mkPPart("p_20210104", 200))
                .set_timestamp(datetime(2021, 1, 2, tzinfo=timezone.utc))
                .set_important(),
                ChangePlannedPartition(mkTailPart("future")).set_timestamp(
                    datetime(2021, 1, 5, tzinfo=timezone.utc)
                ),
            ],
            planned,
        )

    def test_plan_partition_changes_long_delay(self):
        planned = _plan_partition_changes(
            MockDatabase(),
            Table("table"),
            [
                mkPPart("p_20210101", 100),
                mkPPart("p_20210415", 200),
                mkTailPart("future"),
            ],
            mkPos(50),
            datetime(2021, 3, 31, tzinfo=timezone.utc),
            timedelta(days=7),
            2,
        )

        self.assertEqual(
            planned,
            [
                ChangePlannedPartition(mkPPart("p_20210101", 100)),
                ChangePlannedPartition(mkPPart("p_20210415", 200))
                .set_timestamp(datetime(2021, 6, 28, tzinfo=timezone.utc))
                .set_important(),
                ChangePlannedPartition(mkTailPart("future")).set_timestamp(
                    datetime(2021, 7, 5, tzinfo=timezone.utc)
                ),
            ],
        )

    def test_plan_partition_changes_short_names(self):
        self.maxDiff = None
        planned = _plan_partition_changes(
            MockDatabase(),
            Table("table"),
            [
                mkPPart("p_2019", 1912499867),
                mkPPart("p_2020", 8890030931),
                mkPPart("p_20210125", 12010339136),
                mkTailPart("p_future"),
            ],
            mkPos(10810339136),
            datetime(2021, 1, 30, tzinfo=timezone.utc),
            timedelta(days=7),
            2,
        )

        self.assertEqual(
            planned,
            [
                ChangePlannedPartition(mkPPart("p_20210125", 12010339136)).set_position(
                    [12010339136]
                ),
                ChangePlannedPartition(mkTailPart("p_future"))
                .set_position([12960433003])
                .set_timestamp(datetime(2021, 2, 1, tzinfo=timezone.utc)),
                NewPlannedPartition()
                .set_columns(1)
                .set_timestamp(datetime(2021, 2, 8, tzinfo=timezone.utc)),
            ],
        )

        output = list(
            generate_sql_reorganize_partition_commands(Table("table"), planned)
        )
        self.assertEqual(
            output,
            [
                "ALTER TABLE `table` REORGANIZE PARTITION `p_future` INTO "
                "(PARTITION `p_20210201` VALUES LESS THAN (12960433003), "
                "PARTITION `p_20210208` VALUES LESS THAN MAXVALUE);"
            ],
        )

    def test_plan_partition_changes_bespoke_names(self):
        planned = _plan_partition_changes(
            MockDatabase(),
            Table("table"),
            [mkPPart("p_start", 100), mkTailPart("p_future")],
            mkPos(50),
            datetime(2021, 1, 6, tzinfo=timezone.utc),
            timedelta(days=7),
            2,
        )

        self.assertEqual(
            planned,
            [
                ChangePlannedPartition(mkPPart("p_start", 100)),
                ChangePlannedPartition(mkTailPart("p_future"))
                .set_position([170])
                .set_timestamp(datetime(2021, 1, 8, tzinfo=timezone.utc)),
                NewPlannedPartition()
                .set_columns(1)
                .set_timestamp(datetime(2021, 1, 15, tzinfo=timezone.utc)),
            ],
        )

        output = list(
            generate_sql_reorganize_partition_commands(Table("table"), planned)
        )
        self.assertEqual(
            output,
            [
                "ALTER TABLE `table` REORGANIZE PARTITION `p_future` INTO "
                "(PARTITION `p_20210108` VALUES LESS THAN (170), "
                "PARTITION `p_20210115` VALUES LESS THAN MAXVALUE);"
            ],
        )

    def test_plan_partition_changes(self):
        self.maxDiff = None
        planned = _plan_partition_changes(
            MockDatabase(),
            Table("table"),
            [
                mkPPart("p_20201231", 100),
                mkPPart("p_20210102", 200),
                mkTailPart("future"),
            ],
            mkPos(50),
            datetime(2021, 1, 1, tzinfo=timezone.utc),
            timedelta(days=7),
            2,
        )

        self.assertEqual(
            planned,
            [
                ChangePlannedPartition(mkPPart("p_20201231", 100)),
                ChangePlannedPartition(mkPPart("p_20210102", 200)),
                ChangePlannedPartition(mkTailPart("future")).set_timestamp(
                    datetime(2021, 1, 4, tzinfo=timezone.utc)
                ),
            ],
        )

        self.assertEqual(
            _plan_partition_changes(
                MockDatabase(),
                Table("table"),
                [
                    mkPPart("p_20201231", 100),
                    mkPPart("p_20210102", 200),
                    mkTailPart("future"),
                ],
                mkPos(199),
                datetime(2021, 1, 3, tzinfo=timezone.utc),
                timedelta(days=7),
                3,
            ),
            [
                ChangePlannedPartition(mkPPart("p_20210102", 200)).set_position([200]),
                ChangePlannedPartition(mkTailPart("future"))
                .set_position([320])
                .set_timestamp(datetime(2021, 1, 3, tzinfo=timezone.utc)),
                NewPlannedPartition()
                .set_position([440])
                .set_timestamp(datetime(2021, 1, 10, tzinfo=timezone.utc)),
                NewPlannedPartition()
                .set_columns(1)
                .set_timestamp(datetime(2021, 1, 17, tzinfo=timezone.utc)),
            ],
        )

    def test_plan_partition_changes_misprediction(self):
        """We have to handle the case where the partition list doesn't cleanly
        match reality."""
        self.maxDiff = None
        planned = _plan_partition_changes(
            MockDatabase(),
            Table("table"),
            [
                mkPPart("p_20210505", 9505010028),
                mkPPart("p_20210604", 10152257517),
                mkPPart("p_20210704", 10799505006),
                mkTailPart("p_20210803"),
            ],
            mkPos(10264818175),
            datetime(2021, 6, 8, tzinfo=timezone.utc),
            timedelta(days=30),
            3,
        )

        self.assertEqual(
            planned,
            [
                ChangePlannedPartition(mkPPart("p_20210704", 10799505006)),
                ChangePlannedPartition(mkTailPart("p_20210803"))
                .set_position([11578057459])
                .set_timestamp(datetime(2021, 6, 28, tzinfo=timezone.utc)),
                NewPlannedPartition()
                .set_position([12356609912])
                .set_timestamp(datetime(2021, 7, 28, tzinfo=timezone.utc)),
                NewPlannedPartition()
                .set_columns(1)
                .set_timestamp(datetime(2021, 8, 27, tzinfo=timezone.utc)),
            ],
        )

    def test_get_rate_partitions_with_implicit_timestamps(self):
        eval_time = datetime(2021, 6, 8, tzinfo=timezone.utc)

        partition_list = _get_rate_partitions_with_implicit_timestamps(
            Table("table"),
            [mkPPart("p_20210505", 9505010028), mkPPart("p_20210604", 10152257517)],
            mkPos(10064818175),
            eval_time,
            mkPPart("p_20210704", 10799505006),
        )
        print(partition_list)
        self.assertEqual(
            partition_list,
            [
                mkPPart("p_20210505", 9505010028),
                mkPPart("p_20210604", 10152257517),
                InstantPartition("p_current_pos", eval_time, [10064818175]),
            ],
        )

    def test_get_rate_partitions_with_queried_timestamps_no_query(self):
        with self.assertRaises(ValueError):
            _get_rate_partitions_with_queried_timestamps(
                MockDatabase(),
                Table("table"),
                [mkPPart("p_20210505", 9505010028), mkPPart("p_20210604", 10152257517)],
                mkPos(10064818175),
                datetime(2021, 6, 8, tzinfo=timezone.utc),
                mkPPart("p_20210704", 10799505006),
            )

    def test_get_rate_partitions_with_queried_timestamps(self):
        times = [
            datetime(2021, 3, 8, tzinfo=timezone.utc),
            datetime(2021, 4, 8, tzinfo=timezone.utc),
            datetime(2021, 5, 8, tzinfo=timezone.utc),
        ]
        tbl = Table("table")
        tbl.set_earliest_utc_timestamp_query(
            SqlQuery("SELECT insert_date FROM table WHERE id = ?;")
        )
        database = MockDatabase()
        database.push_response([{"insert_date": times[0].timestamp()}])
        database.push_response([{"insert_date": times[1].timestamp()}])

        partition_list = _get_rate_partitions_with_queried_timestamps(
            database,
            tbl,
            [mkPPart("p_20210505", 9505010028), mkPPart("p_20210604", 10152257517)],
            mkPos(10064818175),
            times[2],
            mkPPart("p_20210704", 10799505006),
        )

        self.assertEqual(
            partition_list,
            [
                InstantPartition("p_20210505", times[0], [9505010028]),
                InstantPartition("p_20210604", times[1], [10152257517]),
                InstantPartition("p_20210704", times[2], [10064818175]),
            ],
        )

    def test_should_run_changes(self):
        self.assertFalse(
            _should_run_changes(
                Table("table"),
                [
                    ChangePlannedPartition(mkPPart("p_20210102", 200)).set_position(
                        [300]
                    )
                ],
            )
        )

        self.assertFalse(
            _should_run_changes(
                Table("table"),
                [
                    ChangePlannedPartition(mkPPart("p_20210102", 200)).set_position(
                        [300]
                    ),
                    ChangePlannedPartition(mkPPart("p_20210109", 1000)).set_position(
                        [1300]
                    ),
                ],
            )
        )
        with self.assertLogs("should_run_changes:table", level="DEBUG") as logctx:
            self.assertTrue(
                _should_run_changes(
                    Table("table"),
                    [
                        ChangePlannedPartition(mkPPart("p_20210102", 200)).set_position(
                            [302]
                        ),
                        ChangePlannedPartition(mkTailPart("future"))
                        .set_position([422])
                        .set_timestamp(datetime(2021, 1, 9, tzinfo=timezone.utc)),
                        NewPlannedPartition()
                        .set_position([542])
                        .set_timestamp(datetime(2021, 1, 16, tzinfo=timezone.utc)),
                        NewPlannedPartition()
                        .set_position([662])
                        .set_timestamp(datetime(2021, 1, 23, tzinfo=timezone.utc)),
                    ],
                )
            )
        self.assertEqual(
            logctx.output,
            [
                "DEBUG:should_run_changes:table:Add: [542] 2021-01-16 "
                "00:00:00+00:00 is new"
            ],
        )

        with self.assertLogs("should_run_changes:table", level="DEBUG") as logctx:
            self.assertTrue(
                _should_run_changes(
                    Table("table"),
                    [
                        ChangePlannedPartition(mkPPart("p_20210102", 200)),
                        NewPlannedPartition()
                        .set_position([542])
                        .set_timestamp(datetime(2021, 1, 16, tzinfo=timezone.utc)),
                        NewPlannedPartition()
                        .set_position([662])
                        .set_timestamp(datetime(2021, 1, 23, tzinfo=timezone.utc)),
                    ],
                )
            )
        self.assertEqual(
            logctx.output,
            [
                "DEBUG:should_run_changes:table:Add: [542] 2021-01-16 "
                "00:00:00+00:00 is new"
            ],
        )

    def testgenerate_sql_reorganize_partition_commands_no_change(self):
        self.assertEqual(
            list(
                generate_sql_reorganize_partition_commands(
                    Table("table"), [ChangePlannedPartition(mkPPart("p_20210102", 200))]
                )
            ),
            [],
        )

    def testgenerate_sql_reorganize_partition_commands_single_change(self):
        self.assertEqual(
            list(
                generate_sql_reorganize_partition_commands(
                    Table("table"),
                    [
                        ChangePlannedPartition(mkPPart("p_20210102", 200, 200))
                        .set_position([542, 190])
                        .set_timestamp(datetime(2021, 1, 16, tzinfo=timezone.utc))
                    ],
                )
            ),
            [
                "ALTER TABLE `table` REORGANIZE PARTITION `p_20210102` INTO "
                "(PARTITION `p_20210116` VALUES LESS THAN (542, 190));"
            ],
        )

    def testgenerate_sql_reorganize_partition_commands_two_changes(self):
        self.assertEqual(
            list(
                generate_sql_reorganize_partition_commands(
                    Table("table"),
                    [
                        ChangePlannedPartition(mkPPart("p_20210102", 200))
                        .set_position([500])
                        .set_timestamp(datetime(2021, 1, 16, tzinfo=timezone.utc)),
                        ChangePlannedPartition(mkPPart("p_20210120", 1000))
                        .set_position([2000])
                        .set_timestamp(datetime(2021, 2, 14, tzinfo=timezone.utc)),
                    ],
                )
            ),
            [
                "ALTER TABLE `table` REORGANIZE PARTITION `p_20210120` INTO "
                "(PARTITION `p_20210214` VALUES LESS THAN (2000));",
                "ALTER TABLE `table` REORGANIZE PARTITION `p_20210102` INTO "
                "(PARTITION `p_20210116` VALUES LESS THAN (500));",
            ],
        )

    def testgenerate_sql_reorganize_partition_commands_new_partitions(self):
        self.assertEqual(
            list(
                generate_sql_reorganize_partition_commands(
                    Table("table"),
                    [
                        ChangePlannedPartition(mkPPart("p_20210102", 200)),
                        NewPlannedPartition()
                        .set_position([542])
                        .set_timestamp(datetime(2021, 1, 16, tzinfo=timezone.utc)),
                        NewPlannedPartition()
                        .set_position([662])
                        .set_timestamp(datetime(2021, 1, 23, tzinfo=timezone.utc)),
                    ],
                )
            ),
            [
                "ALTER TABLE `table` REORGANIZE PARTITION `p_20210102` INTO "
                "(PARTITION `p_20210102` VALUES LESS THAN (200), "
                "PARTITION `p_20210116` VALUES LESS THAN (542), "
                "PARTITION `p_20210123` VALUES LESS THAN (662));"
            ],
        )

    def testgenerate_sql_reorganize_partition_commands_maintain_new_partition(self):
        self.assertEqual(
            list(
                generate_sql_reorganize_partition_commands(
                    Table("table"),
                    [
                        ChangePlannedPartition(mkTailPart("future"))
                        .set_position([800])
                        .set_timestamp(datetime(2021, 1, 14, tzinfo=timezone.utc)),
                        NewPlannedPartition()
                        .set_position([1000])
                        .set_timestamp(datetime(2021, 1, 16, tzinfo=timezone.utc)),
                        NewPlannedPartition()
                        .set_position([1200])
                        .set_timestamp(datetime(2021, 1, 23, tzinfo=timezone.utc)),
                        NewPlannedPartition()
                        .set_columns(1)
                        .set_timestamp(datetime(2021, 1, 30, tzinfo=timezone.utc)),
                    ],
                )
            ),
            [
                "ALTER TABLE `table` REORGANIZE PARTITION `future` INTO "
                "(PARTITION `p_20210114` VALUES LESS THAN (800), "
                "PARTITION `p_20210116` VALUES LESS THAN (1000), "
                "PARTITION `p_20210123` VALUES LESS THAN (1200), "
                "PARTITION `p_20210130` VALUES LESS THAN MAXVALUE);"
            ],
        )

    def testgenerate_sql_reorganize_partition_commands_with_duplicate(self):
        with self.assertRaises(DuplicatePartitionException):
            list(
                generate_sql_reorganize_partition_commands(
                    Table("table_with_duplicate"),
                    [
                        ChangePlannedPartition(mkTailPart("future"))
                        .set_position([800])
                        .set_timestamp(datetime(2021, 1, 14, tzinfo=timezone.utc)),
                        NewPlannedPartition()
                        .set_position([1000])
                        .set_timestamp(datetime(2021, 1, 14, tzinfo=timezone.utc)),
                        NewPlannedPartition()
                        .set_position([1200])
                        .set_timestamp(datetime(2021, 1, 15, tzinfo=timezone.utc)),
                    ],
                )
            )

    def testgenerate_sql_reorganize_partition_commands_out_of_order(self):
        with self.assertRaises(AssertionError):
            list(
                generate_sql_reorganize_partition_commands(
                    Table("table_with_out_of_order_changeset"),
                    [
                        ChangePlannedPartition(mkTailPart("past"))
                        .set_position([800])
                        .set_timestamp(datetime(2021, 1, 14, tzinfo=timezone.utc)),
                        NewPlannedPartition()
                        .set_position([1000])
                        .set_timestamp(datetime(2021, 1, 15, tzinfo=timezone.utc)),
                        ChangePlannedPartition(mkTailPart("future"))
                        .set_position([1200])
                        .set_timestamp(datetime(2021, 1, 16, tzinfo=timezone.utc)),
                    ],
                )
            )

    def test_plan_andgenerate_sql_reorganize_partition_commands_with_future_partition(
        self,
    ):
        planned = _plan_partition_changes(
            MockDatabase(),
            Table("table"),
            [
                mkPPart("p_20201231", 100),
                mkPPart("p_20210104", 200),
                mkTailPart("future"),
            ],
            mkPos(50),
            datetime(2021, 1, 1, tzinfo=timezone.utc),
            timedelta(days=7),
            2,
        )

        self.assertEqual(
            list(generate_sql_reorganize_partition_commands(Table("water"), planned)),
            [
                "ALTER TABLE `water` REORGANIZE PARTITION `future` INTO "
                "(PARTITION `p_20210105` VALUES LESS THAN MAXVALUE);",
                "ALTER TABLE `water` REORGANIZE PARTITION `p_20210104` INTO "
                "(PARTITION `p_20210102` VALUES LESS THAN (200));",
            ],
        )

    def test_get_pending_sql_reorganize_partition_commands_no_changes(self):
        with self.assertLogs(
            "get_pending_sql_reorganize_partition_commands:plushies", level="INFO"
        ) as logctx:
            cmds = get_pending_sql_reorganize_partition_commands(
                database=MockDatabase(),
                table=Table("plushies"),
                partition_list=[
                    mkPPart("p_20201231", 100),
                    mkPPart("p_20210102", 200),
                    mkTailPart("future"),
                ],
                current_position=mkPos(50),
                allowed_lifespan=timedelta(days=7),
                num_empty_partitions=2,
                evaluation_time=datetime(2021, 1, 1, tzinfo=timezone.utc),
            )

        self.assertEqual(
            logctx.output,
            [
                "INFO:get_pending_sql_reorganize_partition_commands:plushies:"
                "Table plushies does not need to be modified currently."
            ],
        )

        self.assertEqual(cmds, [])

    def test_get_pending_sql_reorganize_partition_commands_with_changes(self):
        with self.assertLogs(
            "get_pending_sql_reorganize_partition_commands:plushies", level="DEBUG"
        ) as logctx:
            cmds = get_pending_sql_reorganize_partition_commands(
                database=MockDatabase(),
                table=Table("plushies"),
                partition_list=[
                    mkPPart("p_20201231", 100),
                    mkPPart("p_20210102", 200),
                    mkTailPart("future"),
                ],
                current_position=mkPos(50),
                allowed_lifespan=timedelta(days=7),
                num_empty_partitions=4,
                evaluation_time=datetime(2021, 1, 1, tzinfo=timezone.utc),
            )

        self.assertEqual(
            logctx.output,
            [
                "DEBUG:get_pending_sql_reorganize_partition_commands:plushies:"
                "Table plushies has changes waiting."
            ],
        )

        self.assertEqual(
            list(cmds),
            [
                "ALTER TABLE `plushies` REORGANIZE PARTITION `future` INTO "
                "(PARTITION `p_20210104` VALUES LESS THAN (550), "
                "PARTITION `p_20210111` VALUES LESS THAN (900), "
                "PARTITION `p_20210118` VALUES LESS THAN MAXVALUE);"
            ],
        )

    def test_get_columns(self):
        db = MockDatabase()
        expected = [{"Field": "id", "Type": "int"}, {"Field": "day", "Type": "int"}]
        db.push_response(expected)

        self.assertEqual(expected, get_columns(db, Table("table")))

    def test_parse_columns(self):
        column_descriptions = [
            {"Field": "id", "Type": "int", "Extra": 3},
            {"Field": "day", "Type": "int"},
        ]
        self.assertEqual(
            _parse_columns(Table("table"), column_descriptions), column_descriptions
        )

        with self.assertRaises(TableInformationException):
            _parse_columns(Table("table"), [])

        with self.assertRaises(TableInformationException):
            _parse_columns(Table("table"), [{"Type": "melee_range"}])

        with self.assertRaises(TableInformationException):
            _parse_columns(Table("table"), [{"Field": "five_feet"}])

        with self.assertRaises(TableInformationException):
            _parse_columns(Table("table"), [{"Field": "five_feet"}])


if __name__ == "__main__":
    unittest.main()

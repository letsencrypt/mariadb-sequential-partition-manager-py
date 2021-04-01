import tempfile
import unittest
import pymysql
from datetime import datetime, timezone
from pathlib import Path
from .cli import (
    all_configured_tables_are_compatible,
    config_from_args,
    do_partition,
    parser,
    partition_cmd,
    stats_cmd,
)

fake_exec = Path(__file__).absolute().parent.parent / "test_tools/fake_mariadb.sh"
nonexistant_exec = fake_exec.parent / "not_real"


def insert_into_file(fp, data):
    fp.write(data.encode("utf-8"))
    fp.seek(0)


def run_partition_cmd_yaml(yaml):
    with tempfile.NamedTemporaryFile() as tmpfile:
        insert_into_file(tmpfile, yaml)
        args = parser.parse_args(["add", "--config", tmpfile.name])
        return partition_cmd(args)


def partition_cmd_at_time(args, time):
    conf = config_from_args(args)
    conf.curtime = time
    return do_partition(conf)


class TestPartitionCmd(unittest.TestCase):
    maxDiff = None

    def test_partition_cmd_no_exec(self):
        args = parser.parse_args(
            [
                "--mariadb",
                str(nonexistant_exec),
                "add",
                "--noop",
                "--table",
                "testtable",
            ]
        )
        with self.assertRaises(FileNotFoundError):
            partition_cmd(args)

    def test_partition_cmd_noop(self):
        args = parser.parse_args(
            ["--mariadb", str(fake_exec), "add", "--noop", "--table", "testtable_noop"]
        )
        output = partition_cmd_at_time(args, datetime(2020, 11, 8, tzinfo=timezone.utc))

        self.assertEqual(
            {
                "testtable_noop": {
                    "sql": (
                        "ALTER TABLE `testtable_noop` REORGANIZE PARTITION "
                        "`p_20201204` INTO "
                        "(PARTITION `p_20201205` VALUES LESS THAN (548), "
                        "PARTITION `p_20210104` VALUES LESS THAN MAXVALUE);"
                    ),
                    "noop": True,
                }
            },
            output,
        )

    def test_partition_cmd_final(self):
        args = parser.parse_args(
            ["--mariadb", str(fake_exec), "add", "--table", "testtable_commit"]
        )
        output = partition_cmd_at_time(args, datetime(2020, 11, 8, tzinfo=timezone.utc))

        self.assertEqual(
            {
                "testtable_commit": {
                    "output": [],
                    "sql": (
                        "ALTER TABLE `testtable_commit` REORGANIZE PARTITION "
                        "`p_20201204` INTO "
                        "(PARTITION `p_20201205` VALUES LESS THAN (548), "
                        "PARTITION `p_20210104` VALUES LESS THAN MAXVALUE);"
                    ),
                }
            },
            output,
        )

    def test_partition_cmd_several_tables(self):
        args = parser.parse_args(
            [
                "--mariadb",
                str(fake_exec),
                "add",
                "--table",
                "testtable",
                "another_table",
            ]
        )
        output = partition_cmd(args)

        self.assertEqual(len(output), 2)
        self.assertSequenceEqual(list(output), ["testtable", "another_table"])

    def test_partition_unpartitioned_table(self):
        o = run_partition_cmd_yaml(
            f"""
partitionmanager:
    tables:
        test:
        unpartitioned:
    mariadb: {str(fake_exec)}
"""
        )
        self.assertSequenceEqual(list(o), [])

    def test_partition_cmd_invalid_yaml(self):
        with self.assertRaises(TypeError):
            run_partition_cmd_yaml(
                """
data:
    tables:
        what
"""
            )

    def test_partition_cmd_no_tables(self):
        with self.assertRaises(TypeError):
            run_partition_cmd_yaml(
                f"""
partitionmanager:
    mariadb: {str(fake_exec)}
    tables:
"""
            )

    def test_partition_cmd_one_table(self):
        o = run_partition_cmd_yaml(
            f"""
partitionmanager:
    mariadb: {str(fake_exec)}
    tables:
        test_with_retention:
            retention:
                days: 10
"""
        )
        self.assertSequenceEqual(list(o), ["test_with_retention"])

    def test_partition_cmd_two_tables(self):
        o = run_partition_cmd_yaml(
            f"""
partitionmanager:
    tables:
        test:
        test_with_retention:
            retention:
                days: 10
    mariadb: {str(fake_exec)}
"""
        )
        self.assertSequenceEqual(list(o), ["test", "test_with_retention"])

    def test_partition_period_daily(self):
        o = run_partition_cmd_yaml(
            f"""
partitionmanager:
    partition_period:
        days: 1
    tables:
        partitioned_last_week:
        partitioned_yesterday:
    mariadb: {str(fake_exec)}
"""
        )
        self.assertSequenceEqual(
            list(o), ["partitioned_last_week", "partitioned_yesterday"]
        )

    def test_partition_period_seven_days(self):
        o = run_partition_cmd_yaml(
            f"""
partitionmanager:
    num_empty: 1
    partition_period:
        days: 7
    tables:
        partitioned_yesterday:
        partitioned_last_week:
    mariadb: {str(fake_exec)}
"""
        )
        self.assertSequenceEqual(list(o), [])

    def test_partition_period_different_per_table(self):
        o = run_partition_cmd_yaml(
            f"""
partitionmanager:
    partition_period:
        days: 7
    tables:
        partitioned_yesterday:
            partition_period:
                days: 1
        partitioned_last_week:
    mariadb: {str(fake_exec)}
"""
        )
        self.assertSequenceEqual(
            list(o), ["partitioned_yesterday", "partitioned_last_week"]
        )

    def test_partition_with_db_url(self):
        with self.assertRaises(pymysql.err.OperationalError):
            run_partition_cmd_yaml(
                """
partitionmanager:
    tables:
        test:
        unpartitioned:
    dburl: sql://user@localhost:9999/fake_database
"""
            )


class TestStatsCmd(unittest.TestCase):
    def test_stats(self):
        args = parser.parse_args(
            ["--mariadb", str(fake_exec), "stats", "--table", "partitioned_yesterday"]
        )
        r = stats_cmd(args)
        self.assertEqual(r["partitioned_yesterday"]["partitions"], 3)
        self.assertLess(
            r["partitioned_yesterday"]["time_since_newest_partition"].days, 2
        )
        self.assertLess(
            r["partitioned_yesterday"]["time_since_oldest_partition"].days, 43
        )
        self.assertGreater(r["partitioned_yesterday"]["mean_partition_delta"].days, 2)
        self.assertGreater(r["partitioned_yesterday"]["max_partition_delta"].days, 2)


class TestHelpers(unittest.TestCase):
    def test_all_configured_tables_are_compatible_one(self):
        args = parser.parse_args(
            ["--mariadb", str(fake_exec), "stats", "--table", "partitioned_yesterday"]
        )
        config = config_from_args(args)
        self.assertTrue(all_configured_tables_are_compatible(config))

    def test_all_configured_tables_are_compatible_three(self):
        args = parser.parse_args(
            [
                "--mariadb",
                str(fake_exec),
                "stats",
                "--table",
                "partitioned_last_week",
                "partitioned_yesterday",
                "othertable",
            ]
        )
        config = config_from_args(args)
        self.assertTrue(all_configured_tables_are_compatible(config))

    def test_all_configured_tables_are_compatible_three_one_unpartitioned(self):
        args = parser.parse_args(
            [
                "--mariadb",
                str(fake_exec),
                "stats",
                "--table",
                "partitioned_last_week",
                "unpartitioned",
                "othertable",
            ]
        )
        config = config_from_args(args)
        self.assertFalse(all_configured_tables_are_compatible(config))

    def test_all_configured_tables_are_compatible_unpartitioned(self):
        args = parser.parse_args(
            ["--mariadb", str(fake_exec), "stats", "--table", "unpartitioned"]
        )
        config = config_from_args(args)
        self.assertFalse(all_configured_tables_are_compatible(config))

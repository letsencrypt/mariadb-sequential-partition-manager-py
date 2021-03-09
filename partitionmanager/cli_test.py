import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from .cli import (
    all_configured_tables_are_compatible,
    config_from_args,
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


class TestPartitionCmd(unittest.TestCase):
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
            ["--mariadb", str(fake_exec), "add", "--noop", "--table", "testtable"]
        )
        output = partition_cmd(args)

        expectedDate = datetime.now(tz=timezone.utc).strftime("p_%Y%m%d")

        self.assertEqual(
            "ALTER TABLE `testtable` REORGANIZE PARTITION `p_20201204` INTO "
            + f"(PARTITION `p_20201204` VALUES LESS THAN (3101009), PARTITION `{expectedDate}` "
            + "VALUES LESS THAN MAXVALUE);",
            output["testtable"]["sql"],
        )

    def test_partition_cmd_final(self):
        args = parser.parse_args(
            ["--mariadb", str(fake_exec), "add", "--table", "testtable"]
        )
        output = partition_cmd(args)

        expectedDate = datetime.now(tz=timezone.utc).strftime("p_%Y%m%d")

        self.assertEqual(
            {
                "testtable": {
                    "output": [],
                    "sql": "ALTER TABLE `testtable` REORGANIZE PARTITION `p_20201204` "
                    + "INTO (PARTITION `p_20201204` VALUES LESS THAN (3101009), "
                    + f"PARTITION `{expectedDate}` VALUES LESS THAN MAXVALUE);",
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

    def test_partition_duration_daily(self):
        o = run_partition_cmd_yaml(
            f"""
partitionmanager:
    partition_duration:
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

    def test_partition_duration_seven_days(self):
        o = run_partition_cmd_yaml(
            f"""
partitionmanager:
    partition_duration:
        days: 7
    tables:
        partitioned_yesterday:
        partitioned_last_week:
    mariadb: {str(fake_exec)}
"""
        )
        self.assertSequenceEqual(list(o), ["partitioned_last_week"])


class TestStatsCmd(unittest.TestCase):
    def test_stats(self):
        args = parser.parse_args(
            ["--mariadb", str(fake_exec), "stats", "--table", "partitioned_yesterday"]
        )
        r = stats_cmd(args)
        self.assertEqual(r["partitioned_yesterday"]["partitions"], 3)
        self.assertLess(r["partitioned_yesterday"]["time_since_last_partition"].days, 2)
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

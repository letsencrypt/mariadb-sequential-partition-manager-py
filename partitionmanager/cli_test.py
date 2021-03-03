import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from .cli import parser, partition_cmd

fake_exec = Path(__file__).absolute().parent.parent / "test_tools/fake_mariadb.sh"
nonexistant_exec = fake_exec.parent / "not_real"


def insert_into_file(fp, data):
    fp.write(data.encode("utf-8"))
    fp.seek(0)


def run_partition_cmd_yaml(yaml):
    with tempfile.NamedTemporaryFile() as tmpfile:
        insert_into_file(tmpfile, yaml)
        args = parser.parse_args(["add_partition", "--config", tmpfile.name])
        return partition_cmd(args)


class TestPartitionCmd(unittest.TestCase):
    def test_partition_cmd_no_exec(self):
        args = parser.parse_args(
            [
                "--mariadb",
                str(nonexistant_exec),
                "add_partition",
                "--noop",
                "--table",
                "testtable",
            ]
        )
        with self.assertRaises(FileNotFoundError):
            partition_cmd(args)

    def test_partition_cmd_noop(self):
        args = parser.parse_args(
            [
                "--mariadb",
                str(fake_exec),
                "add_partition",
                "--noop",
                "--table",
                "testtable",
            ]
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
            ["--mariadb", str(fake_exec), "add_partition", "--table", "testtable"]
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
                "add_partition",
                "--table",
                "testtable",
                "another_table",
            ]
        )
        output = partition_cmd(args)

        self.assertEqual(len(output), 2)
        for k in output.keys():
            self.assertTrue(k in ["testtable", "another_table"])

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

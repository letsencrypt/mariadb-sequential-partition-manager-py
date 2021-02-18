import unittest
from datetime import datetime, timezone
from pathlib import Path
from .cli import parser, partition_cmd

fake_exec = Path(__file__).absolute().parent.parent / "test_tools/fake_mariadb.sh"
nonexistant_exec = fake_exec.parent / "not_real"


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

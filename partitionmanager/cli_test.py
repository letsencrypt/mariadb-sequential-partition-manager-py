import unittest
from pathlib import Path
from .cli import parser, partition_cmd

fake_exec = Path(__file__).absolute().parent.parent / "test_tools/fake_mariadb.sh"
nonexistant_exec = fake_exec.parent / "not_real"


class TestPartitionCmd(unittest.TestCase):
    def test_partition_cmd_no_exec(self):
        args = parser.parse_args(
            [
                "--db",
                "testdb",
                "--table",
                "testtable",
                "--mariadb",
                str(nonexistant_exec),
                "add_partition",
                "--noop",
            ]
        )
        with self.assertRaises(FileNotFoundError):
            partition_cmd(args)

    def test_partition_cmd_noop(self):
        args = parser.parse_args(
            [
                "--db",
                "testdb",
                "--table",
                "testtable",
                "--mariadb",
                str(fake_exec),
                "add_partition",
                "--noop",
            ]
        )
        output = partition_cmd(args)

        self.assertEqual(
            "ALTER TABLE `testdb`.`testtable` REORGANIZE PARTITION `p_20201204` INTO "
            + "(PARTITION `p_20201204` VALUES LESS THAN (3101009), PARTITION `p_20210122` "
            + "VALUES LESS THAN MAXVALUE);",
            output,
        )

    def test_partition_cmd_final(self):
        args = parser.parse_args(
            [
                "--db",
                "testdb",
                "--table",
                "testtable",
                "--mariadb",
                str(fake_exec),
                "add_partition",
            ]
        )
        output = partition_cmd(args)

        self.assertEqual("", output)

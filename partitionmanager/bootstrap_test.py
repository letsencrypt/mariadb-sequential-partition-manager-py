import io
import unittest
import yaml
from datetime import datetime, timedelta

from .bootstrap import (
    _generate_sql_copy_commands,
    _get_time_offsets,
    _suffix,
    _trigger_column_copies,
    _override_config_to_map_data,
    _plan_partitions_for_time_offsets,
    calculate_sql_alters_from_state_info,
    write_state_info,
)
from .cli import Config
from .types import (
    DatabaseCommand,
    Table,
    SqlInput,
    MaxValuePartition,
    ChangePlannedPartition,
    NewPlannedPartition,
)


class MockDatabase(DatabaseCommand):
    def __init__(self):
        self._response = list()
        self._select_response = [[{"id": 150}]]
        self.num_queries = 0

    def run(self, cmd):
        self.num_queries += 1

        if "CREATE_OPTIONS" in cmd:
            return [{"CREATE_OPTIONS": "partitioned"}]

        if "SHOW CREATE TABLE" in cmd:
            return [
                {
                    "Create Table": """CREATE TABLE `burgers` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`),
) ENGINE=InnoDB AUTO_INCREMENT=150 DEFAULT CHARSET=utf8
 PARTITION BY RANGE (`id`)
(PARTITION `p_start` VALUES LESS THAN MAXVALUE ENGINE = InnoDB)"""
                }
            ]

        if "SELECT" in cmd:
            return self._select_response.pop()

        return self._response.pop()

    def db_name(self):
        return SqlInput("the-database")


class TestBootstrapTool(unittest.TestCase):
    def test_writing_state_info(self):
        conf = Config()
        conf.curtime = datetime(2021, 3, 1)
        conf.dbcmd = MockDatabase()
        conf.tables = [Table("test")]

        out = io.StringIO()

        write_state_info(conf, out)

        written_yaml = yaml.safe_load(out.getvalue())

        self.assertEqual(
            written_yaml, {"tables": {"test": {"id": 150}}, "time": conf.curtime}
        )

    def test_get_time_offsets(self):
        self.assertEqual(
            _get_time_offsets(1, timedelta(hours=4), timedelta(days=30)),
            [timedelta(hours=4)],
        )

        self.assertEqual(
            _get_time_offsets(2, timedelta(hours=4), timedelta(days=30)),
            [timedelta(hours=4), timedelta(days=30, hours=4)],
        )

        self.assertEqual(
            _get_time_offsets(3, timedelta(hours=4), timedelta(days=30)),
            [
                timedelta(hours=4),
                timedelta(days=30, hours=4),
                timedelta(days=60, hours=4),
            ],
        )

    def test_read_state_info(self):
        self.maxDiff = None
        conf_past = Config()
        conf_past.curtime = datetime(2021, 3, 1)
        conf_past.dbcmd = MockDatabase()
        conf_past.tables = [Table("test").set_partition_period(timedelta(days=30))]

        state_fs = io.StringIO()
        yaml.dump({"tables": {"test": {"id": 0}}, "time": conf_past.curtime}, state_fs)
        state_fs.seek(0)

        with self.assertRaises(ValueError):
            calculate_sql_alters_from_state_info(conf_past, state_fs)

        conf_now = Config()
        conf_now.curtime = datetime(2021, 3, 3)
        conf_now.dbcmd = MockDatabase()
        conf_now.dbcmd._response = [
            [
                {"Field": "id", "Type": "bigint UNSIGNED"},
                {"Field": "serial", "Type": "varchar"},
            ]
        ]
        conf_now.tables = [Table("test").set_partition_period(timedelta(days=30))]

        state_fs.seek(0)
        x = calculate_sql_alters_from_state_info(conf_now, state_fs)
        self.assertEqual(
            x,
            {
                "test": [
                    "DROP TABLE IF EXISTS test_new_20210303;",
                    "CREATE TABLE test_new_20210303 LIKE test;",
                    "ALTER TABLE test_new_20210303 REMOVE PARTITIONING;",
                    "ALTER TABLE test_new_20210303 PARTITION BY RANGE(id) (",
                    "\tPARTITION p_start VALUES LESS THAN MAXVALUE",
                    ");",
                    "ALTER TABLE `test_new_20210303` REORGANIZE PARTITION `p_start` "
                    + "INTO (PARTITION `p_20210303` VALUES LESS THAN (156), "
                    + "PARTITION `p_20210402` VALUES LESS THAN (2406), PARTITION "
                    + "`p_20210502` VALUES LESS THAN MAXVALUE);",
                    "CREATE OR REPLACE TRIGGER copy_inserts_from_test_to_test_new_20210303",
                    "\tAFTER INSERT ON test FOR EACH ROW",
                    "\t\tINSERT INTO test_new_20210303 SET",
                    "\t\t\t`id` = NEW.`id`,",
                    "\t\t\t`serial` = NEW.`serial`;",
                    "CREATE OR REPLACE TRIGGER copy_updates_from_test_to_test_new_20210303",
                    "\tAFTER UPDATE ON test FOR EACH ROW",
                    "\t\tUPDATE test_new_20210303 SET",
                    "\t\t\t`serial` = NEW.`serial`",
                    "\t\tWHERE `id` = NEW.`id`;",
                ]
            },
        )

    def test_read_state_info_map_table(self):
        self.maxDiff = None
        conf = Config()
        conf.assume_partitioned_on = ["order", "auth"]
        conf.curtime = datetime(2021, 3, 3)
        conf.dbcmd = MockDatabase()
        conf.dbcmd._select_response = [[{"auth": 22}], [{"order": 11}]]
        conf.dbcmd._response = [
            [
                {"Field": "order", "Type": "bigint UNSIGNED"},
                {"Field": "auth", "Type": "bigint UNSIGNED"},
            ]
        ]
        conf.tables = [Table("map_table").set_partition_period(timedelta(days=30))]

        state_fs = io.StringIO()
        yaml.dump(
            {
                "tables": {"map_table": {"order": 11, "auth": 22}},
                "time": (conf.curtime - timedelta(days=1)),
            },
            state_fs,
        )
        state_fs.seek(0)

        x = calculate_sql_alters_from_state_info(conf, state_fs)
        print(x)
        self.assertEqual(
            x,
            {
                "map_table": [
                    "DROP TABLE IF EXISTS map_table_new_20210303;",
                    "CREATE TABLE map_table_new_20210303 LIKE map_table;",
                    "ALTER TABLE map_table_new_20210303 REMOVE PARTITIONING;",
                    "ALTER TABLE map_table_new_20210303 PARTITION BY RANGE(order, auth) (",
                    "\tPARTITION p_assumed VALUES LESS THAN MAXVALUE",
                    ");",
                    "ALTER TABLE `map_table_new_20210303` REORGANIZE PARTITION "
                    + "`p_assumed` INTO (PARTITION `p_20210303` VALUES LESS THAN "
                    + "(11, 22), PARTITION `p_20210402` VALUES LESS THAN "
                    + "(11, 22), PARTITION `p_20210502` VALUES LESS THAN "
                    + "MAXVALUE, MAXVALUE);",
                    "CREATE OR REPLACE TRIGGER copy_inserts_from_map_table_"
                    + "to_map_table_new_20210303",
                    "\tAFTER INSERT ON map_table FOR EACH ROW",
                    "\t\tINSERT INTO map_table_new_20210303 SET",
                    "\t\t\t`auth` = NEW.`auth`,",
                    "\t\t\t`order` = NEW.`order`;",
                ]
            },
        )

    def test_trigger_column_copies(self):
        self.assertEqual(list(_trigger_column_copies([])), [])
        self.assertEqual(list(_trigger_column_copies(["a"])), ["`a` = NEW.`a`"])
        self.assertEqual(
            list(_trigger_column_copies(["b", "a", "c"])),
            ["`b` = NEW.`b`", "`a` = NEW.`a`", "`c` = NEW.`c`"],
        )

    def test_suffix(self):
        self.assertEqual(list(_suffix(["a"])), ["a"])
        self.assertEqual(list(_suffix(["a", "b"])), ["a", "b"])
        self.assertEqual(list(_suffix(["a", "b"], indent=" ")), [" a", " b"])
        self.assertEqual(list(_suffix(["a", "b"], mid_suffix=",")), ["a,", "b"])
        self.assertEqual(list(_suffix(["a", "b"], final_suffix=";")), ["a", "b;"])
        self.assertEqual(
            list(_suffix(["a", "b"], mid_suffix=",", final_suffix=";")), ["a,", "b;"]
        )

    def test_generate_sql_copy_commands(self):
        conf = Config()
        conf.assume_partitioned_on = ["id"]
        conf.curtime = datetime(2021, 3, 3)
        conf.dbcmd = MockDatabase()
        map_data = _override_config_to_map_data(conf)
        cmds = list(
            _generate_sql_copy_commands(
                Table("old"),
                map_data,
                ["id", "field"],
                Table("new"),
                ["STRAIGHT_UP_INSERTED", "STUFF GOES HERE"],
            )
        )

        print(cmds)
        self.assertEqual(
            cmds,
            [
                "DROP TABLE IF EXISTS new;",
                "CREATE TABLE new LIKE old;",
                "ALTER TABLE new REMOVE PARTITIONING;",
                "ALTER TABLE new PARTITION BY RANGE(id) (",
                "\tPARTITION p_assumed VALUES LESS THAN MAXVALUE",
                ");",
                "STRAIGHT_UP_INSERTED",
                "STUFF GOES HERE",
                "CREATE OR REPLACE TRIGGER copy_inserts_from_old_to_new",
                "\tAFTER INSERT ON old FOR EACH ROW",
                "\t\tINSERT INTO new SET",
                "\t\t\t`field` = NEW.`field`,",
                "\t\t\t`id` = NEW.`id`;",
                "CREATE OR REPLACE TRIGGER copy_updates_from_old_to_new",
                "\tAFTER UPDATE ON old FOR EACH ROW",
                "\t\tUPDATE new SET",
                "\t\t\t`field` = NEW.`field`",
                "\t\tWHERE `id` = NEW.`id`;",
            ],
        )

    def test_plan_partitions_for_time_offsets(self):
        parts = _plan_partitions_for_time_offsets(
            datetime(2021, 3, 3),
            [timedelta(days=60), timedelta(days=360)],
            [11943234],
            [16753227640],
            MaxValuePartition("p_assumed", count=1),
        )
        self.assertIsInstance(parts[0], ChangePlannedPartition)
        self.assertIsInstance(parts[1], NewPlannedPartition)

#!/usr/bin/env python3

import argparse
import logging
import traceback
import yaml

from datetime import datetime, timedelta, timezone
from partitionmanager.table_append_partition import (
    assert_table_is_compatible,
    evaluate_partition_actions,
    format_sql_reorganize_partition_command,
    get_current_positions,
    get_partition_map,
    parition_name_now,
    reorganize_partition,
)
from partitionmanager.types import (
    SqlInput,
    Table,
    retention_from_dict,
    toSqlUrl,
    TableInformationException,
)
from partitionmanager.sql import SubprocessDatabaseCommand, IntegratedDatabaseCommand

parser = argparse.ArgumentParser(
    description="""
    In already-partitioned tables with an auto_increment key as the partition,
    add a new partition at the current auto_increment value.
"""
)

parser.add_argument(
    "--log-level",
    default=logging.INFO,
    type=lambda x: getattr(logging, x.upper()),
    help="Configure the logging level.",
)

group = parser.add_mutually_exclusive_group()
group.add_argument("--mariadb", default="mariadb", help="Path to mariadb command")
group.add_argument(
    "--dburl",
    type=toSqlUrl,
    help="DB connection url, such as sql://user:pass@10.0.0.1:3306/database",
)


class Config:
    def __init__(self):
        self.tables = list()
        self.dbcmd = SubprocessDatabaseCommand("mariadb")
        self.noop = False
        self.curtime = datetime.now(tz=timezone.utc)
        self.partition_duration = timedelta(days=30)

    def from_argparse(self, args):
        if args.table:
            for n in args.table:
                self.tables.append(Table(n))
        if args.dburl:
            self.dbcmd = IntegratedDatabaseCommand(args.dburl)
        else:
            self.dbcmd = SubprocessDatabaseCommand(args.mariadb)
        if args.days:
            self.partition_duration = timedelta(days=args.days)
            if self.partition_duration <= timedelta():
                raise ValueError("Negative lifespan is not allowed")
        self.noop = args.noop

    def from_yaml_file(self, file):
        data = yaml.safe_load(file)
        if "partitionmanager" not in data:
            raise TypeError(
                "Unexpected YAML format: missing top-level partitionmanager"
            )
        data = data["partitionmanager"]
        if "tables" not in data or not isinstance(data["tables"], dict):
            raise TypeError("Unexpected YAML format: no tables defined")
        if "noop" in data:
            self.noop = data["noop"]
        if "partition_duration" in data:
            self.partition_duration = retention_from_dict(data["partition_duration"])
            if self.partition_duration <= timedelta():
                raise ValueError("Negative lifespan is not allowed")
        if "dburl" in data:
            self.dbcmd = IntegratedDatabaseCommand(data["dburl"])
        elif "mariadb" in data:
            self.dbcmd = SubprocessDatabaseCommand(data["mariadb"])
        for key in data["tables"]:
            t = Table(key)
            tabledata = data["tables"][key]
            if isinstance(tabledata, dict) and "retention" in tabledata:
                t.set_retention(retention_from_dict(tabledata["retention"]))

            self.tables.append(t)


def partition_cmd(args):
    conf = Config()
    conf.from_argparse(args)
    if args.config:
        conf.from_yaml_file(args.config)
    if conf.noop:
        logging.info("No-op mode")

    # Preflight
    try:
        for table in conf.tables:
            assert_table_is_compatible(conf.dbcmd, table)
    except TableInformationException as tie:
        logging.error(f"Cannot proceed: {tie}")
        return {}

    all_results = dict()
    for table in conf.tables:
        map_data = get_partition_map(conf.dbcmd, table)

        decision = evaluate_partition_actions(
            map_data["partitions"], conf.curtime, conf.partition_duration
        )

        if not decision["do_partition"]:
            logging.info(
                f"{table} does not need to be partitioned. "
                f"(Next partition: {decision['remaining_lifespan']})"
            )
            continue
        logging.debug(
            f"{table} is ready to partition (Lifespan: {decision['remaining_lifespan']})"
        )

        positions = get_current_positions(conf.dbcmd, table, map_data["range_cols"])

        filled_partition_id, partitions = reorganize_partition(
            map_data["partitions"], parition_name_now(), positions
        )

        sql_cmd = format_sql_reorganize_partition_command(
            table, partition_to_alter=filled_partition_id, partition_list=partitions
        )

        if conf.noop:
            all_results[table.name] = {"sql": sql_cmd}
            logging.info(f"{table} planned SQL: {sql_cmd}")
            continue

        logging.info(f"{table} running SQL: {sql_cmd}")
        output = conf.dbcmd.run(sql_cmd)
        all_results[table.name] = {"sql": sql_cmd, "output": output}
        logging.info(f"{table} results: {output}")
    return all_results


subparsers = parser.add_subparsers(dest="subparser_name")
partition_parser = subparsers.add_parser("add", help="add partitions")
partition_parser.add_argument(
    "--noop",
    "-n",
    action="store_true",
    help="Don't attempt to commit changes, just print",
)
partition_parser.add_argument(
    "--days", "-d", type=int, help="Lifetime of each partition in days"
)
partition_group = partition_parser.add_mutually_exclusive_group()
partition_group.add_argument(
    "--config", "-c", type=argparse.FileType("r"), help="Configuration YAML"
)
partition_group.add_argument(
    "--table", "-t", type=SqlInput, nargs="+", help="table names"
)
partition_parser.set_defaults(func=partition_cmd)


def main():
    """
    Start here.
    """
    args = parser.parse_args()
    logging.basicConfig(level=args.log_level)
    if "func" not in args:
        parser.print_help()
        return

    try:
        output = args.func(args)
        for k, v in output.items():
            print(f"{k}: {v}")
    except Exception:
        logging.warning(f"Couldn't complete command: {args.subparser_name}")
        logging.warning(traceback.format_exc())


if __name__ == "__main__":
    main()

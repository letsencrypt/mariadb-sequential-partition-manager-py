#!/usr/bin/env python3

import argparse
import logging
import subprocess
import traceback

from partitionmanager.table_append_partition import (
    get_partition_map,
    get_autoincrement,
    reorganize_partition,
    format_sql_reorganize_partition_command,
)
from partitionmanager.types import DatabaseCommand, SqlInput


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
parser.add_argument("--mariadb", default="mariadb", help="Path to mariadb command")
# parser.add_argument("--user", help="database username")
parser.add_argument("--db", type=SqlInput, help="database name", required=True)
parser.add_argument("--table", "-t", type=SqlInput, help="table name", required=True)


def partition_cmd(args):
    dbcmd = SubprocessDatabaseCommand(args.mariadb)

    ai = get_autoincrement(dbcmd, args.db, args.table)

    partitions = get_partition_map(dbcmd, args.db, args.table)

    filled_partition_id, partitions = reorganize_partition(partitions, ai)

    sql_cmd = format_sql_reorganize_partition_command(
        args.db,
        args.table,
        partition_to_alter=filled_partition_id,
        partition_list=partitions,
    )

    if args.noop:
        logging.info("No-op mode")
        return sql_cmd

    logging.info("Executing " + sql_cmd)
    results = dbcmd.run(sql_cmd)
    logging.info("Results:")
    logging.info(results)
    return results


subparsers = parser.add_subparsers(dest="subparser_name")

partition_parser = subparsers.add_parser("add_partition", help="add a partition")
partition_parser.set_defaults(func=partition_cmd)
partition_parser.add_argument(
    "--noop",
    "-n",
    action="store_true",
    help="Don't attempt to commit changes, just print",
)


class SubprocessDatabaseCommand(DatabaseCommand):
    def __init__(self, exe):
        self.exe = exe

    def run(self, sql_cmd):
        result = subprocess.run(
            [self.exe, "-E"],
            input=sql_cmd,
            stdout=subprocess.PIPE,
            encoding="UTF-8",
            check=True,
        )
        return result.stdout


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
        print(args.func(args))
    except Exception:
        logging.warning(f"Couldn't complete command: {args.subparser_name}")
        logging.warning(traceback.format_exc())


if __name__ == "__main__":
    main()

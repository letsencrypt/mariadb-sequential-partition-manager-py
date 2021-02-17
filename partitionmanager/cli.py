#!/usr/bin/env python3

import argparse
import logging
import traceback

from partitionmanager.table_append_partition import (
    get_partition_map,
    get_autoincrement,
    reorganize_partition,
    format_sql_reorganize_partition_command,
)
from partitionmanager.types import SqlInput, toSqlUrl
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


def partition_cmd(args):
    if args.dburl:
        dbcmd = IntegratedDatabaseCommand(args.dburl)
    else:
        dbcmd = SubprocessDatabaseCommand(args.mariadb)

    for table in args.table:
        ai = get_autoincrement(dbcmd, table)

        partitions = get_partition_map(dbcmd, table)

        filled_partition_id, partitions = reorganize_partition(partitions, ai)

        sql_cmd = format_sql_reorganize_partition_command(
            table, partition_to_alter=filled_partition_id, partition_list=partitions
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
partition_parser.add_argument(
    "--noop",
    "-n",
    action="store_true",
    help="Don't attempt to commit changes, just print",
)
partition_parser.add_argument(
    "--table", "-t", type=SqlInput, nargs="+", help="table names", required=True
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
        print(args.func(args))
    except Exception:
        logging.warning(f"Couldn't complete command: {args.subparser_name}")
        logging.warning(traceback.format_exc())


if __name__ == "__main__":
    main()

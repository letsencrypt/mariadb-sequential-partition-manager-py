#!/usr/bin/env python3

import argparse
import logging
import traceback
import yaml

from datetime import datetime, timedelta, timezone
from pathlib import Path
from partitionmanager.table_append_partition import (
    evaluate_partition_actions,
    format_sql_reorganize_partition_command,
    get_current_positions,
    get_partition_map,
    partition_name_now,
    reorganize_partition,
    table_is_compatible,
)
from partitionmanager.types import SqlInput, Table, retention_from_dict, toSqlUrl
from partitionmanager.stats import get_statistics, PrometheusMetrics
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
parser.add_argument(
    "--prometheus-stats", type=Path, help="Path to produce a prometheus statistics file"
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
        self.partition_period = timedelta(days=30)
        self.prometheus_stats_path = None

    def from_argparse(self, args):
        if args.table:
            for n in args.table:
                self.tables.append(Table(n))
        if args.dburl:
            self.dbcmd = IntegratedDatabaseCommand(args.dburl)
        else:
            self.dbcmd = SubprocessDatabaseCommand(args.mariadb)
        if "days" in args and args.days:
            self.partition_period = timedelta(days=args.days)
            if self.partition_period <= timedelta():
                raise ValueError("Negative lifespan is not allowed")
        if "noop" in args:
            self.noop = args.noop
        if "prometheus_stats" in args:
            self.prometheus_stats_path = args.prometheus_stats

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
        if "partition_period" in data:
            self.partition_period = retention_from_dict(data["partition_period"])
            if self.partition_period <= timedelta():
                raise ValueError("Negative lifespan is not allowed")
        if "dburl" in data:
            self.dbcmd = IntegratedDatabaseCommand(toSqlUrl(data["dburl"]))
        elif "mariadb" in data:
            self.dbcmd = SubprocessDatabaseCommand(data["mariadb"])
        for key in data["tables"]:
            t = Table(key)
            tabledata = data["tables"][key]
            if isinstance(tabledata, dict) and "retention" in tabledata:
                t.set_retention(retention_from_dict(tabledata["retention"]))
            if isinstance(tabledata, dict) and "partition_period" in tabledata:
                t.set_partition_period(
                    retention_from_dict(tabledata["partition_period"])
                )

            self.tables.append(t)
        if "prometheus_stats" in data:
            self.prometheus_stats_path = Path(data["prometheus_stats"])


def config_from_args(args):
    conf = Config()
    conf.from_argparse(args)
    if args.config:
        conf.from_yaml_file(args.config)
    return conf


def all_configured_tables_are_compatible(conf):
    problems = dict()
    for table in conf.tables:
        problem = table_is_compatible(conf.dbcmd, table)
        if problem:
            problems[table.name] = problem
            logging.error(f"Cannot proceed: {table} {problem}")

    return len(problems) == 0


def partition_cmd(args):
    conf = config_from_args(args)
    return do_partition(conf)


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


def stats_cmd(args):
    conf = config_from_args(args)
    return do_stats(conf)


stats_parser = subparsers.add_parser("stats", help="get stats for partitions")
stats_group = stats_parser.add_mutually_exclusive_group()
stats_group.add_argument(
    "--config", "-c", type=argparse.FileType("r"), help="Configuration YAML"
)
stats_group.add_argument("--table", "-t", type=SqlInput, nargs="+", help="table names")
stats_parser.set_defaults(func=stats_cmd)


def do_partition(conf):
    if conf.noop:
        logging.info("No-op mode")

    # Preflight
    if not all_configured_tables_are_compatible(conf):
        return dict()

    metrics = PrometheusMetrics()
    metrics.describe(
        "alter_time_seconds",
        help_text="Time in seconds to complete the ALTER command",
        type="gauge",
    )

    all_results = dict()
    for table in conf.tables:
        map_data = get_partition_map(conf.dbcmd, table)

        duration = conf.partition_period
        if table.partition_period:
            duration = table.partition_period

        decision = evaluate_partition_actions(
            map_data["partitions"], conf.curtime, duration
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
            map_data["partitions"], partition_name_now(), positions
        )

        sql_cmd = format_sql_reorganize_partition_command(
            table, partition_to_alter=filled_partition_id, partition_list=partitions
        )

        if conf.noop:
            all_results[table.name] = {"sql": sql_cmd}
            logging.info(f"{table} planned SQL: {sql_cmd}")
            continue

        logging.info(f"{table} running SQL: {sql_cmd}")
        time_start = datetime.utcnow()
        output = conf.dbcmd.run(sql_cmd)
        time_end = datetime.utcnow()

        all_results[table.name] = {"sql": sql_cmd, "output": output}
        logging.info(f"{table} results: {output}")
        metrics.add(
            "alter_time_seconds", table.name, (time_end - time_start).total_seconds()
        )

    if conf.prometheus_stats_path:
        do_stats(conf, metrics)

    return all_results


def do_stats(conf, metrics=PrometheusMetrics()):
    # Preflight
    if not all_configured_tables_are_compatible(conf):
        return dict()

    all_results = dict()
    for table in conf.tables:
        map_data = get_partition_map(conf.dbcmd, table)
        statistics = get_statistics(map_data["partitions"], conf.curtime, table)
        all_results[table.name] = statistics

    if conf.prometheus_stats_path:
        metrics.describe(
            "total", help_text="Total number of partitions", type="counter"
        )
        metrics.describe(
            "time_since_newest_partition_seconds",
            help_text="The age in seconds of the last partition for the table",
            type="gauge",
        )
        metrics.describe(
            "time_since_oldest_partition_seconds",
            help_text="The age in seconds of the first partition for the table",
            type="gauge",
        )
        metrics.describe(
            "mean_delta_seconds",
            help_text="Mean seconds between partitions",
            type="gauge",
        )
        metrics.describe(
            "max_delta_seconds",
            help_text="Maximum seconds between partitions",
            type="gauge",
        )

        for table, results in all_results.items():
            if "partitions" in results:
                metrics.add("total", table, results["partitions"])
            if "time_since_newest_partition" in results:
                metrics.add(
                    "time_since_newest_partition_seconds",
                    table,
                    results["time_since_newest_partition"].total_seconds(),
                )
            if "time_since_oldest_partition" in results:
                metrics.add(
                    "time_since_oldest_partition_seconds",
                    table,
                    results["time_since_oldest_partition"].total_seconds(),
                )
            if "mean_partition_delta" in results:
                metrics.add(
                    "mean_delta_seconds",
                    table,
                    results["mean_partition_delta"].total_seconds(),
                )
            if "max_partition_delta" in results:
                metrics.add(
                    "max_delta_seconds",
                    table,
                    results["max_partition_delta"].total_seconds(),
                )

        with conf.prometheus_stats_path.open(mode="w", encoding="utf-8") as sf:
            metrics.render(sf)

    return all_results


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

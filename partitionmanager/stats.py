import logging

from datetime import datetime, timedelta
from itertools import tee
from .types import MaxValuePartition, Partition, UnexpectedPartitionException


class PrometheusMetric:
    def __init__(self, name, table, data):
        self.name = name
        self.table = table
        self.data = data


class PrometheusMetrics:
    def __init__(self):
        self.ts = datetime.utcnow().timestamp() * 1000
        self.metrics = dict()
        self.help = dict()
        self.types = dict()

    def add(self, name, table, data):
        if name not in self.metrics:
            self.metrics[name] = list()
        self.metrics[name].append(PrometheusMetric(name, table, data))

    def describe(self, name, help_text=None, type=None):
        self.help[name] = help_text
        self.types[name] = type

    def render(self, fp):
        # Format specification:
        # https://prometheus.io/docs/instrumenting/exposition_formats/
        for n, metrics in self.metrics.items():
            name = f"partition_{n}"
            if n in self.help:
                print(f"# HELP {name} {self.help[n]}", file=fp)
            if n in self.types:
                print(f"# TYPE {name} {self.types[n]}", file=fp)
            for m in metrics:
                labels = [f'table="{m.table}"']
                print(f"{name}{{{','.join(labels)}}} {m.data} {int(self.ts)}", file=fp)


def pairwise(iterable):
    """
    iterable -> (s0,s1), (s1,s2), (s2, s3), ...
    """
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def get_statistics(partitions, current_timestamp, table):
    results = {"partitions": len(partitions)}

    if not partitions:
        return results

    for p in partitions:
        if not isinstance(p, Partition):
            logging.warning(
                f"{table} get_statistics called with a partition list "
                + f"that included a non-Partition entry: {p}"
            )
            raise UnexpectedPartitionException(p)

    head_part = None
    tail_part = partitions[-1]

    if not isinstance(tail_part, MaxValuePartition):
        logging.warning(
            f"{table} get_statistics called with a partition list tail "
            + f"that wasn't a MaxValuePartition: {p}"
        )
        raise UnexpectedPartitionException(tail_part)

    if tail_part.timestamp():
        results["time_since_newest_partition"] = (
            current_timestamp - tail_part.timestamp()
        )

    for p in partitions:
        if p.timestamp():
            head_part = p
            break

    if not head_part or head_part == tail_part:
        return results

    if head_part.timestamp():
        results["time_since_oldest_partition"] = (
            current_timestamp - head_part.timestamp()
        )

    if head_part.timestamp() and tail_part.timestamp():
        results["mean_partition_delta"] = (
            tail_part.timestamp() - head_part.timestamp()
        ) / (len(partitions) - 1)

    max_d = timedelta()
    for a, b in pairwise(partitions):
        if not a.timestamp() or not b.timestamp():
            logging.debug(f"{table} had partitions that aren't comparable: {a} and {b}")
            continue
        d = b.timestamp() - a.timestamp()
        if d > max_d:
            max_d = d

    if max_d > timedelta():
        results["max_partition_delta"] = max_d

    return results

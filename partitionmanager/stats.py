import logging

from datetime import timedelta
from itertools import tee
from .types import MaxValuePartition, Partition, UnexpectedPartitionException


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

    head_part = partitions[0]
    tail_part = partitions[-1]

    for p in partitions:
        if not isinstance(p, Partition):
            logging.warning(
                f"{table} get_statistics called with a partition list "
                + f"that included a non-Partition entry: {p}"
            )
            raise UnexpectedPartitionException(p)

    if not isinstance(tail_part, MaxValuePartition):
        logging.warning(
            f"{table} get_statistics called with a partition list tail "
            + f"that wasn't a MaxValuePartition: {p}"
        )
        raise UnexpectedPartitionException(tail_part)

    if tail_part.timestamp():
        results["time_since_last_partition"] = current_timestamp - tail_part.timestamp()

    if head_part == tail_part:
        return results

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

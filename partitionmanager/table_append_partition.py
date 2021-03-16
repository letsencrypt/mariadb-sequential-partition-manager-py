from partitionmanager.types import (
    DuplicatePartitionException,
    MaxValuePartition,
    MismatchedIdException,
    NoEmptyPartitionsAvailableException,
    Partition,
    PositionPartition,
    SqlInput,
    Table,
    TableInformationException,
    UnexpectedPartitionException,
)
from .tools import pairwise

from datetime import datetime, timedelta, timezone
import logging
import operator
import re


def table_is_compatible(database, table):
    """
    Gather the information schema from the database command and parse out the
    autoincrement value.
    """
    db_name = database.db_name()

    if (
        type(db_name) != SqlInput
        or type(table) != Table
        or type(table.name) != SqlInput
    ):
        return f"Unexpected table type: {table}"
    sql_cmd = (
        "SELECT CREATE_OPTIONS FROM INFORMATION_SCHEMA.TABLES "
        + f"WHERE TABLE_SCHEMA='{db_name}' and TABLE_NAME='{table.name}';"
    ).strip()

    return table_information_schema_is_compatible(database.run(sql_cmd), table.name)


def table_information_schema_is_compatible(rows, table_name):
    """
    Parse a table information schema, validating options
    """
    if len(rows) != 1:
        return f"Unable to read information for {table_name}"

    options = rows[0]
    if "partitioned" not in options["CREATE_OPTIONS"]:
        return f"Table {table_name} is not partitioned"

    return None


def get_current_positions(database, table, columns):
    """
    Get the positions of the columns provided in the given table, return
    as a list in the same order as the provided columns
    """
    if type(columns) is not list or type(table) is not Table:
        raise ValueError("columns must be a list and table must be a Table")

    order_col = columns[0]
    columns_str = ", ".join([f"`{x}`" for x in columns])
    sql = f"SELECT {columns_str} FROM `{table.name}` ORDER BY {order_col} DESC LIMIT 1;"
    rows = database.run(sql)
    if len(rows) > 1:
        raise TableInformationException(f"Expected one result from {table.name}")
    if len(rows) == 0:
        raise TableInformationException(
            f"Table {table.name} appears to be empty. (No results)"
        )
    ordered_positions = list()
    for c in columns:
        ordered_positions.append(rows[0][c])
    return ordered_positions


def get_partition_map(database, table):
    """
    Gather the partition map via the database command tool.
    """
    if type(table) != Table or type(table.name) != SqlInput:
        raise ValueError("Unexpected type")
    sql_cmd = f"SHOW CREATE TABLE `{table.name}`;".strip()
    return parse_partition_map(database.run(sql_cmd))


def parse_partition_map(rows):
    """
    Read a partition statement from a table creation string and produce Partition
    objets for each partition.
    """
    partition_range = re.compile(
        r"[ ]*PARTITION BY RANGE\s+(COLUMNS)?\((?P<cols>[\w,` ]+)\)"
    )
    partition_member = re.compile(
        r"[ (]*PARTITION\s+`(?P<name>\w+)` VALUES LESS THAN \((?P<cols>[\d, ]+)\)"
    )
    partition_tail = re.compile(
        r"[ (]*PARTITION\s+`(?P<name>\w+)` VALUES LESS THAN \(?(MAXVALUE[, ]*)+\)?"
    )

    range_cols = None
    partitions = list()

    if len(rows) != 1:
        raise TableInformationException("Expected one result")

    options = rows[0]

    for l in options["Create Table"].split("\n"):
        range_match = partition_range.match(l)
        if range_match:
            range_cols = [x.strip("` ") for x in range_match.group("cols").split(",")]
            logging.debug(f"Partition range columns: {range_cols}")

        member_match = partition_member.match(l)
        if member_match:
            part_name = member_match.group("name")
            part_vals_str = member_match.group("cols")
            logging.debug(f"Found partition {part_name} = {part_vals_str}")

            part_vals = [int(x.strip("` ")) for x in part_vals_str.split(",")]

            if range_cols is None:
                raise TableInformationException(
                    "Processing partitions, but the partition definition wasn't found."
                )

            if len(part_vals) != len(range_cols):
                logging.error(
                    f"Partition columns {part_vals} don't match the partition range {range_cols}"
                )
                raise MismatchedIdException("Partition columns mismatch")

            pos_part = PositionPartition(part_name)
            for v in part_vals:
                pos_part.add_position(v)

            partitions.append(pos_part)

        member_tail = partition_tail.match(l)
        if member_tail:
            if range_cols is None:
                raise TableInformationException(
                    "Processing tail, but the partition definition wasn't found."
                )
            part_name = member_tail.group("name")
            logging.debug(f"Found tail partition named {part_name}")
            partitions.append(MaxValuePartition(part_name, len(range_cols)))

    if not partitions or not isinstance(partitions[-1], MaxValuePartition):
        raise UnexpectedPartitionException("There was no tail partition")

    return {"range_cols": range_cols, "partitions": partitions}


def evaluate_partition_actions(partitions, timestamp, allowed_lifespan):
    tail_part = partitions[-1]
    if not isinstance(tail_part, MaxValuePartition):
        raise UnexpectedPartitionException(tail_part)

    if not tail_part.timestamp():
        logging.warning(f"Partition {tail_part} is assumed to need partitioning")
        return {"do_partition": True, "remaining_lifespan": timedelta()}

    lifespan = timestamp - tail_part.timestamp()
    return {
        "do_partition": lifespan >= allowed_lifespan,
        "remaining_lifespan": allowed_lifespan - lifespan,
    }


def partition_name_now():
    """
    Format a partition name for now
    """
    return datetime.now(tz=timezone.utc).strftime("p_%Y%m%d")


def split_partitions_around_positions(partition_list, current_positions):
    """
    Split a partition_list into those for which _all_ values are less than
    current_positions, and all the others.
    """
    for p in partition_list:
        if not isinstance(p, Partition):
            raise UnexpectedPartitionException(p)
    if type(current_positions) is not list:
        raise ValueError()

    less_than_partitions = list()
    greater_or_equal_partitions = list()

    for p in partition_list:
        if p < current_positions:
            less_than_partitions.append(p)
        else:
            greater_or_equal_partitions.append(p)

    return less_than_partitions, greater_or_equal_partitions


def get_position_increase_per_day(p1, p2):
    """
    Return a list containing the change in positions between p1 and p2 divided
    by the number of days between them, as "position increase per day", or raise
    ValueError if p1 is not before p2, or if either p1 or p2 does not have a
    position. For partitions with only a single position, this will be a list of
    size 1.
    """
    if not isinstance(p1, PositionPartition) or not isinstance(p2, PositionPartition):
        raise ValueError("Both partitions must be PositionPartition type")
    if p1.timestamp() >= p2.timestamp():
        raise ValueError("p1 must be before p2")
    if p1.num_columns != p2.num_columns:
        raise ValueError("p1 and p2 must have the same number of columns")
    delta_time = p2.timestamp() - p1.timestamp()
    delta_days = delta_time / timedelta(days=1)
    delta_positions = list(map(operator.sub, p2.positions, p1.positions))
    return list(map(lambda pos: pos / delta_days, delta_positions))


def generate_weights(count):
    """
    Generate a static list of geometricly-decreasing values, starting from
    10,000 to give a high ceiling. It could be dynamic, but eh.
    """
    return [10_000 / x for x in range(count, 0, -1)]


def get_weighted_position_increase_per_day_for_partitions(partitions):
    """
    For the provided list of partitions, uses the get_position_increase_per_day
    method to generate a list position increment rates in positions/day, then
    uses a geometric weight to make more recent rates influence the outcome
    more, and returns a final list of weighted partition-position-increase-per-
    day, with one entry per column.
    """
    pos_rates = [
        get_position_increase_per_day(p1, p2) for p1, p2 in pairwise(partitions)
    ]
    weights = generate_weights(len(pos_rates))

    # Initialize a list with a zero for each position
    weighted_sums = [0] * partitions[0].num_columns

    for p_r, weight in zip(pos_rates, weights):
        for idx, val in enumerate(p_r):
            weighted_sums[idx] += val * weight

    return list(map(lambda x: x / sum(weights), weighted_sums))


def plan_partition_changes(partition_list, current_positions):
    """
    Produces a list of partitions that should be modified or created in order
    to meet the supplied table requirements, using a guess as to the rate of
    fill.
    """
    non_empty_partitions, empty_partitions = split_partitions_around_positions(
        partition_list, current_positions
    )
    if not empty_partitions:
        raise NoEmptyPartitionsAvailableException()


def reorganize_partition(partition_list, new_partition_name, partition_positions):
    """
    From a partial partitions list of Partition types add a new partition at the
    partition_positions, which must be a list.
    """
    if type(partition_positions) is not list:
        raise ValueError()

    num_partition_ids = partition_list[0].num_columns

    tail_part = partition_list.pop()
    if not isinstance(tail_part, MaxValuePartition):
        raise UnexpectedPartitionException(tail_part)
    if tail_part.name == new_partition_name:
        raise DuplicatePartitionException(tail_part)

    # Check any remaining partitions in the list after popping off the tail
    # to make sure each entry has the same number of partition IDs as the first
    # entry.
    for p in partition_list:
        if len(p.positions) != num_partition_ids:
            raise MismatchedIdException(
                "Didn't get the same number of partition IDs: "
                + f"{p} has {len(p)} while expected {num_partition_ids}"
            )
    if len(partition_positions) != num_partition_ids:
        raise MismatchedIdException(
            f"Provided {len(partition_positions)} partition IDs,"
            + f" but expected {num_partition_ids}"
        )

    altered_partition = PositionPartition(tail_part.name)
    for p in partition_positions:
        altered_partition.add_position(p)

    new_partition = MaxValuePartition(new_partition_name, num_partition_ids)

    reorganized_list = [altered_partition, new_partition]
    return altered_partition.name, reorganized_list


def format_sql_reorganize_partition_command(
    table, *, partition_to_alter, partition_list
):
    """
    Produce a SQL command to reorganize the partition in table_name to
    match the new partition_list.
    """
    partition_strings = list()
    for p in partition_list:
        if not isinstance(p, Partition):
            raise UnexpectedPartitionException(p)
        partition_strings.append(f"PARTITION `{p.name}` VALUES LESS THAN {p.values()}")
    partition_update = ", ".join(partition_strings)

    return (
        f"ALTER TABLE `{table.name}` "
        f"REORGANIZE PARTITION `{partition_to_alter}` INTO ({partition_update});"
    )

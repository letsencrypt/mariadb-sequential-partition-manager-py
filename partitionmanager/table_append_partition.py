"""
Design and perform partition management.
"""

from datetime import timedelta
import logging
import operator
import re

from partitionmanager.types import (
    ChangePlannedPartition,
    DuplicatePartitionException,
    InstantPartition,
    MaxValuePartition,
    MismatchedIdException,
    NewPlannedPartition,
    NoEmptyPartitionsAvailableException,
    Partition,
    PlannedPartition,
    PositionPartition,
    SqlInput,
    Table,
    TableInformationException,
    UnexpectedPartitionException,
)
from .tools import pairwise, iter_show_end


def table_is_compatible(database, table):
    """
    Gather the information schema from the database command and parse out the
    autoincrement value.
    """
    db_name = database.db_name()

    if (
        not isinstance(db_name, SqlInput)
        or not isinstance(table, Table)
        or not isinstance(table.name, SqlInput)
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
    if not isinstance(columns, list) or not isinstance(table, Table):
        raise ValueError("columns must be a list and table must be a Table")

    order_col = columns[0]
    columns_str = ", ".join([f"`{x}`" for x in columns])
    sql = f"SELECT {columns_str} FROM `{table.name}` ORDER BY {order_col} DESC LIMIT 1;"
    rows = database.run(sql)
    if len(rows) > 1:
        raise TableInformationException(f"Expected one result from {table.name}")
    if not rows:
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
    if not isinstance(table, Table) or not isinstance(table.name, SqlInput):
        raise ValueError("Unexpected type")
    sql_cmd = f"SHOW CREATE TABLE `{table.name}`;"
    return parse_partition_map(database.run(sql_cmd))


def parse_partition_map(rows):
    """
    Read a partition statement from a table creation string and produce Partition
    objets for each partition.
    """
    log = logging.getLogger("parse_partition_map")

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
            log.debug(f"Partition range columns: {range_cols}")

        member_match = partition_member.match(l)
        if member_match:
            part_name = member_match.group("name")
            part_vals_str = member_match.group("cols")
            log.debug(f"Found partition {part_name} = {part_vals_str}")

            part_vals = [int(x.strip("` ")) for x in part_vals_str.split(",")]

            if range_cols is None:
                raise TableInformationException(
                    "Processing partitions, but the partition definition wasn't found."
                )

            if len(part_vals) != len(range_cols):
                log.error(
                    f"Partition columns {part_vals} don't match the partition range {range_cols}"
                )
                raise MismatchedIdException("Partition columns mismatch")

            pos_part = PositionPartition(part_name).set_position(part_vals)
            partitions.append(pos_part)

        member_tail = partition_tail.match(l)
        if member_tail:
            if range_cols is None:
                raise TableInformationException(
                    "Processing tail, but the partition definition wasn't found."
                )
            part_name = member_tail.group("name")
            log.debug(f"Found tail partition named {part_name}")
            partitions.append(MaxValuePartition(part_name, len(range_cols)))

    if not partitions or not isinstance(partitions[-1], MaxValuePartition):
        raise UnexpectedPartitionException("There was no tail partition")

    return {"range_cols": range_cols, "partitions": partitions}


def split_partitions_around_positions(partition_list, current_positions):
    """
    Split a partition_list into those for which _all_ values are less than
    current_positions, a single partition whose values contain current_positions,
    and a list of all the others.
    """
    for p in partition_list:
        if not isinstance(p, Partition):
            raise UnexpectedPartitionException(p)
    if not isinstance(current_positions, list):
        raise ValueError()

    less_than_partitions = list()
    greater_or_equal_partitions = list()

    for p in partition_list:
        if p < current_positions:
            less_than_partitions.append(p)
        else:
            greater_or_equal_partitions.append(p)

    # The active partition is always the first in the list of greater_or_equal
    active_partition = greater_or_equal_partitions.pop(0)

    return less_than_partitions, active_partition, greater_or_equal_partitions


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
    if None in (p1.timestamp(), p2.timestamp()):
        # An empty list skips this pair in get_weighted_position_increase
        return list()
    if p1.timestamp() >= p2.timestamp():
        raise ValueError(f"p1 {p1} must be before p2 {p2}")
    if p1.num_columns != p2.num_columns:
        raise ValueError(f"p1 {p1} and p2 {p2} must have the same number of columns")
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
    if not partitions:
        raise ValueError("Partition list must not be empty")

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


def predict_forward_position(current_positions, rate_of_change, duration):
    """
    Move current_positions forward a given duration at the provided rates of
    change. The rate and the duration must be compatible units, and both the
    positions and the rate must be lists of the same size.
    """
    if len(current_positions) != len(rate_of_change):
        raise ValueError("Expected identical list sizes")

    for neg_rate in filter(lambda r: r < 0, rate_of_change):
        raise ValueError(
            f"Can't predict forward with a negative rate of change: {neg_rate}"
        )

    increase = list(map(lambda x: x * duration / timedelta(days=1), rate_of_change))
    predicted_positions = [int(p + i) for p, i in zip(current_positions, increase)]
    for old, new in zip(current_positions, predicted_positions):
        assert new >= old, f"Always predict forward, {new} < {old}"
    return predicted_positions


def predict_forward_time(current_positions, end_positions, rates, evaluation_time):
    """
    Given the current_positions and the rates, determine the timestamp of when
    the positions will reach ALL end_positions.
    """
    if not len(current_positions) == len(end_positions) == len(rates):
        raise ValueError("Expected identical list sizes")

    for neg_rate in filter(lambda r: r < 0, rates):
        raise ValueError(
            f"Can't predict forward with a negative rate of change: {neg_rate}"
        )

    days_remaining = [
        (end - now) / rate
        for now, end, rate in zip(current_positions, end_positions, rates)
    ]

    if max(days_remaining) < 0:
        raise ValueError(f"All values are negative: {days_remaining}")

    return evaluation_time + (max(days_remaining) * timedelta(days=1))


def calculate_start_time(last_changed_time, evaluation_time, allowed_lifespan):
    """
    Partition start times should never be in the past.
    """
    partition_start_time = last_changed_time + allowed_lifespan
    if partition_start_time < evaluation_time:
        return evaluation_time
    return partition_start_time


def plan_partition_changes(
    partition_list,
    current_positions,
    evaluation_time,
    allowed_lifespan,
    num_empty_partitions,
):
    """
    Produces a list of partitions that should be modified or created in order
    to meet the supplied table requirements, using an estimate as to the rate of
    fill.
    """
    log = logging.getLogger("plan_partition_changes")

    filled_partitions, active_partition, empty_partitions = split_partitions_around_positions(
        partition_list, current_positions
    )
    if not empty_partitions:
        log.warning(
            f"Partition {active_partition.name} requires manual ALTER "
            "as this tool won't bisect the partition to determine a"
            "rate of fill to make a prediction for new partitions."
        )
        raise NoEmptyPartitionsAvailableException()
    if not active_partition:
        raise Exception("Active Partition can't be None")

    if active_partition.timestamp() >= evaluation_time:
        raise ValueError(
            f"Evaluation time ({evaluation_time}) must be after "
            f"the active partition {active_partition}."
        )

    # This bit of weirdness is a fencepost issue: The partition list is strictly
    # increasing until we get to "now" and the active partition. "Now" actually
    # takes place _after_ active partition's start date (naturally), but
    # contains a position that is before the top of active, by definition. For
    # the rate processing to work, we need to cross the "now" and the active
    # partition's dates and positions.
    rate_relevant_partitions = filled_partitions + [
        InstantPartition(active_partition.timestamp(), current_positions),
        InstantPartition(evaluation_time, active_partition.positions),
    ]
    rates = get_weighted_position_increase_per_day_for_partitions(
        rate_relevant_partitions
    )
    log.debug(
        f"Rates of change calculated as {rates} per day from "
        f"{len(rate_relevant_partitions)} partitions"
    )

    # We need to include active_partition in the list for the subsequent
    # calculations even though we're not actually changing it.
    results = [ChangePlannedPartition(active_partition)]

    # Adjust each of the empty partitions
    for partition in empty_partitions:
        last_changed = results[-1]

        changed_partition = ChangePlannedPartition(partition)

        if isinstance(partition, PositionPartition):
            # We can't change the position on this partition, but we can adjust
            # the name to be more exact as to what date we expect it to begin
            # filling. If we calculate the start-of-fill date and it doesn't
            # match the partition's name, let's rename it and mark it as an
            # important change.
            start_of_fill_time = predict_forward_time(
                current_positions, last_changed.positions, rates, evaluation_time
            )

            if start_of_fill_time.date() != partition.timestamp().date():
                log.info(
                    f"Start-of-fill predicted at {start_of_fill_time.date()} "
                    f"which is not {partition.timestamp().date()}. This change "
                    f"will be marked as important to ensure that {partition} is "
                    f"moved to {start_of_fill_time:%Y-%m-%d}"
                )
                changed_partition.set_timestamp(start_of_fill_time).set_important()

        if isinstance(partition, MaxValuePartition):
            # Only the tail MaxValuePartitions can get new positions. For those,
            # we calculate forward what position we expect and use it in the
            # future.

            partition_start_time = calculate_start_time(
                last_changed.timestamp(), evaluation_time, allowed_lifespan
            )
            changed_part_pos = predict_forward_position(
                last_changed.positions, rates, allowed_lifespan
            )
            changed_partition.set_position(changed_part_pos).set_timestamp(
                partition_start_time
            )

        results.append(changed_partition)

    # Ensure we have the required number of empty partitions
    while len(results) < num_empty_partitions + 1:
        last_changed = results[-1]
        partition_start_time = calculate_start_time(
            last_changed.timestamp(), evaluation_time, allowed_lifespan
        )

        new_part_pos = predict_forward_position(
            last_changed.positions, rates, allowed_lifespan
        )
        results.append(
            NewPlannedPartition()
            .set_position(new_part_pos)
            .set_timestamp(partition_start_time)
        )

    # Final result is always MAXVALUE
    results[-1].set_as_max_value()

    log.debug(f"Planned {results}")

    return results


def evaluate_partition_changes(altered_partitions):
    """
    Evaluate the list from plan_partition_changes and determine if the set of
    changes should be performed - if all the changes are minor, they shouldn't
    be run. Returns True if the changeset should run, otherwise logs the reason
    for skipping and returns False
    """
    log = logging.getLogger("evaluate_partition_changes")

    for p in altered_partitions:
        if isinstance(p, NewPlannedPartition):
            log.debug(f"{p} is new")
            return True

        if isinstance(p, ChangePlannedPartition):
            if p.important():
                log.debug(f"{p} is marked important")
                return True

    return False


def generate_sql_reorganize_partition_commands(table, changes):
    """
    Generate a series of SQL commands to reorganize the partition in table_name
    to match the new changes list.
    """
    log = logging.getLogger(f"generate_sql_reorganize_partition_commands:{table.name}")

    modified_partitions = list()
    new_partitions = list()

    for p in changes:
        if not isinstance(p, PlannedPartition):
            raise UnexpectedPartitionException(p)
        if isinstance(p, NewPlannedPartition):
            new_partitions.append(p)
        else:
            modified_partitions.append(p)

    # If there's not at least one modification, bail out
    if not new_partitions and not list(
        filter(lambda x: x.has_modifications, modified_partitions)
    ):
        log.debug("No partitions have modifications and no new partitions")
        return

    new_part_list = list()
    partition_names_set = set()

    for modified_partition, is_final in reversed(
        list(iter_show_end(modified_partitions))
    ):
        # We reverse the iterator so that we always alter the furthest-out partitions
        # first, so that we are always increasing the number of empty partitions
        # before (potentially) moving the end position near the active one
        new_part_list = [modified_partition.as_partition()]
        if is_final:
            new_part_list.extend([p.as_partition() for p in new_partitions])

        # If there's not at least one modification, skip
        if not is_final and not modified_partition.has_modifications:
            log.debug(f"{modified_partition} does not have modifications, skip")
            continue

        partition_strings = list()
        for part in new_part_list:
            if part.name in partition_names_set:
                raise DuplicatePartitionException(f"Duplicate {part}")
            partition_names_set.add(part.name)

            partition_strings.append(
                f"PARTITION `{part.name}` VALUES LESS THAN {part.values()}"
            )
        partition_update = ", ".join(partition_strings)

        alter_cmd = (
            f"ALTER TABLE `{table.name}` "
            f"REORGANIZE PARTITION `{modified_partition.old.name}` INTO ({partition_update});"
        )

        log.debug(f"Yielding {alter_cmd}")

        yield alter_cmd

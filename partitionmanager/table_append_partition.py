"""
Design and perform partition management.
"""

from datetime import datetime, timedelta, timezone
import logging
import operator
import re

import partitionmanager.types
import partitionmanager.tools


def get_table_compatibility_problems(database, table):
    """Return a list of strings of problems altering this table, or empty."""
    db_name = database.db_name()

    if (
        not isinstance(db_name, partitionmanager.types.SqlInput)
        or not isinstance(table, partitionmanager.types.Table)
        or not isinstance(table.name, partitionmanager.types.SqlInput)
    ):
        return [f"Unexpected table type: {table}"]

    sql_cmd = (
        "SELECT CREATE_OPTIONS FROM INFORMATION_SCHEMA.TABLES "
        + f"WHERE TABLE_SCHEMA='{db_name}' and TABLE_NAME='{table.name}';"
    ).strip()
    return _get_table_information_schema_problems(database.run(sql_cmd), table.name)


def _get_table_information_schema_problems(rows, table_name):
    """Return a string representing problems partitioning this table, or None."""
    if len(rows) != 1:
        return [f"Unable to read information for {table_name}"]

    options = rows[0]
    if "partitioned" not in options["CREATE_OPTIONS"]:
        return [f"Table {table_name} is not partitioned"]
    return list()


def get_current_positions(database, table, columns):
    """Get positions of the columns in the table.

    Return as a dictionary of {column_name: position}
    """
    if not isinstance(columns, list) or not isinstance(
        table, partitionmanager.types.Table
    ):
        raise ValueError("columns must be a list and table must be a Table")

    positions = dict()
    for column in columns:
        if not isinstance(column, str):
            raise ValueError("columns must be a list of strings")
        sql = f"SELECT {column} FROM `{table.name}` ORDER BY {column} DESC LIMIT 1;"
        rows = database.run(sql)
        if len(rows) > 1:
            raise partitionmanager.types.TableInformationException(
                f"Expected one result from {table.name}"
            )
        if not rows:
            raise partitionmanager.types.TableInformationException(
                f"Table {table.name} appears to be empty. (No results)"
            )
        positions[column] = rows[0][column]
    return positions


def get_partition_map(database, table):
    """Gather the partition map via the database command tool."""
    if not isinstance(table, partitionmanager.types.Table) or not isinstance(
        table.name, partitionmanager.types.SqlInput
    ):
        raise ValueError("Unexpected type")
    sql_cmd = f"SHOW CREATE TABLE `{table.name}`;"
    return _parse_partition_map(database.run(sql_cmd))


def _parse_partition_map(rows):
    """Return a dictionary of range_cols and partition objects.

    The "range_cols" is the ordered list of what columns are used as the
    range identifiers for the partitions.

    The "partitions" is a list of the Partition objects representing each
    defined partition. There will be at least one partitionmanager.types.MaxValuePartition.
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
        raise partitionmanager.types.TableInformationException("Expected one result")

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
                raise partitionmanager.types.TableInformationException(
                    "Processing partitions, but the partition definition wasn't found."
                )

            if len(part_vals) != len(range_cols):
                log.error(
                    f"Partition columns {part_vals} don't match the partition range {range_cols}"
                )
                raise partitionmanager.types.MismatchedIdException(
                    "Partition columns mismatch"
                )

            pos_part = partitionmanager.types.PositionPartition(part_name).set_position(
                part_vals
            )
            partitions.append(pos_part)

        member_tail = partition_tail.match(l)
        if member_tail:
            if range_cols is None:
                raise partitionmanager.types.TableInformationException(
                    "Processing tail, but the partition definition wasn't found."
                )
            part_name = member_tail.group("name")
            log.debug(f"Found tail partition named {part_name}")
            partitions.append(
                partitionmanager.types.MaxValuePartition(part_name, len(range_cols))
            )

    if not partitions or not isinstance(
        partitions[-1], partitionmanager.types.MaxValuePartition
    ):
        raise partitionmanager.types.UnexpectedPartitionException(
            "There was no tail partition"
        )
    return {"range_cols": range_cols, "partitions": partitions}


def get_columns(database, table):
    """ Gather the columns list via the database command tool. """
    if not isinstance(table, partitionmanager.types.Table) or not isinstance(
        table.name, partitionmanager.types.SqlInput
    ):
        raise ValueError("Unexpected type")
    sql_cmd = f"DESCRIBE `{table.name}`;"
    return _parse_columns(table, database.run(sql_cmd))


def _parse_columns(table, rows):
    """ Read the columns description and return a list of the columns, where
    each entry is a dict containing Field and Type. """
    log = logging.getLogger("parse_columns")
    if not rows:
        raise partitionmanager.types.TableInformationException("No column information")

    for r in rows:
        if "Field" not in r or "Type" not in r:
            raise partitionmanager.types.TableInformationException(
                "Described table does not include sufficient column details"
            )
        log.debug(f"{table.name} column {r['Field']} has type {r['Type']}")
    return rows


def _split_partitions_around_position(partition_list, current_position):
    """Divide up a partition list to three parts: filled, current, and empty.

    The first part is the filled partition list: those partitions for which
    _all_ values are less than current_position.

    The second is the a single partition whose values contain current_position.

    The third part is a list of all the other, empty partitions yet-to-be-filled.
    """
    for p in partition_list:
        if not partitionmanager.types.is_partition_type(p):
            raise partitionmanager.types.UnexpectedPartitionException(p)
    if not isinstance(current_position, partitionmanager.types.Position):
        raise ValueError()

    less_than_partitions = list()
    greater_or_equal_partitions = list()

    for p in partition_list:
        if p < current_position:
            less_than_partitions.append(p)
        else:
            greater_or_equal_partitions.append(p)

    # The active partition is always the first in the list of greater_or_equal
    active_partition = greater_or_equal_partitions.pop(0)
    return less_than_partitions, active_partition, greater_or_equal_partitions


def _get_position_increase_per_day(p1, p2):
    """Return the rate of change between two position-lists, in positions/day.

    Returns a list containing the change in positions between p1 and p2 divided
    by the number of days between them, as "position increase per day", or raise
    ValueError if p1 is not before p2, or if either p1 or p2 does not have a
    position. For partitions with only a single position, this will be a list of
    size 1.
    """
    log = logging.getLogger("get_position_increase_per_day")

    if not isinstance(p1, partitionmanager.types.PositionPartition) or not isinstance(
        p2, partitionmanager.types.PositionPartition
    ):
        raise ValueError(
            "Both partitions must be partitionmanager.types.PositionPartition type"
        )
    if p1.num_columns != p2.num_columns:
        raise ValueError(f"p1 {p1} and p2 {p2} must have the same number of columns")

    if None in (p1.timestamp(), p2.timestamp()):
        # An empty list skips this pair in get_weighted_position_increase
        return list()
    if p1.timestamp() >= p2.timestamp():
        log.warning(
            f"Skipping rate of change between p1 {p1} and p2 {p2} as they are out-of-order"
        )
        return list()

    delta_time = p2.timestamp() - p1.timestamp()
    delta_days = delta_time / timedelta(days=1)
    delta_positions = list(
        map(operator.sub, p2.position.as_list(), p1.position.as_list())
    )
    return list(map(lambda pos: pos / delta_days, delta_positions))


def _generate_weights(count):
    """Static list of geometrically-decreasing weights.

    Starts from 10,000 to give a high ceiling. It could be dynamic, but eh.
    """
    return [10_000 / x for x in range(count, 0, -1)]


def _get_weighted_position_increase_per_day_for_partitions(partitions):
    """Get weighted partition-position-increase-per-day as a position-list.

    For the provided list of partitions, uses the _get_position_increase_per_day
    method to generate a list position increment rates in positions/day, then
    uses a geometric weight to make more recent rates influence the outcome
    more, and returns a final list of weighted partition-position-increase-per-
    day, with one entry per column.
    """
    if not partitions:
        raise ValueError("Partition list must not be empty")

    pos_rates = [
        _get_position_increase_per_day(p1, p2)
        for p1, p2 in partitionmanager.tools.pairwise(partitions)
    ]
    weights = _generate_weights(len(pos_rates))

    # Initialize a list with a zero for each position
    weighted_sums = [0] * partitions[0].num_columns

    for p_r, weight in zip(pos_rates, weights):
        for idx, val in enumerate(p_r):
            weighted_sums[idx] += val * weight
    return list(map(lambda x: x / sum(weights), weighted_sums))


def _predict_forward_position(current_positions, rate_of_change, duration):
    """Return a predicted future position as a position-list.

    This moves current_positions forward a given duration at the provided rates
    of change. The rate and the duration must be compatible units, and both the
    positions and the rate must be lists of the same size.
    """
    if len(current_positions) != len(rate_of_change):
        raise ValueError("Expected identical list sizes")

    for neg_rate in filter(lambda r: r < 0, rate_of_change):
        raise ValueError(
            f"Can't predict forward with a negative rate of change: {neg_rate}"
        )

    increase = list(map(lambda x: x * (duration / timedelta(days=1)), rate_of_change))
    predicted_positions = [int(p + i) for p, i in zip(current_positions, increase)]
    for old, new in zip(current_positions, predicted_positions):
        assert new >= old, f"Always predict forward, {new} < {old}"
    return predicted_positions


def _predict_forward_time(current_position, end_position, rates, evaluation_time):
    """Return a predicted datetime of when we'll exceed the end position-list.

    Given the current_position position-list and the rates, this calculates
    a timestamp of when the positions will be beyond ALL of the end_positions
    position-list, as that is MariaDB's definition of when to start filling a
    partition.
    """
    if not isinstance(
        current_position, partitionmanager.types.Position
    ) or not isinstance(end_position, partitionmanager.types.Position):
        raise ValueError("Expected to be given Position types")

    if not len(current_position) == len(end_position) == len(rates):
        raise ValueError("Expected identical list sizes")

    for neg_rate in filter(lambda r: r <= 0, rates):
        raise ValueError(
            f"Can't predict forward with a non-positive rate of change: "
            f"{neg_rate} / {rates}"
        )

    days_remaining = [
        (end - now) / rate
        for now, end, rate in zip(
            current_position.as_list(), end_position.as_list(), rates
        )
    ]

    if max(days_remaining) < 0:
        raise ValueError(f"All values are negative: {days_remaining}")
    calculated = evaluation_time + (max(days_remaining) * timedelta(days=1))
    return calculated.replace(minute=0, second=0, microsecond=0)


def _calculate_start_time(last_changed_time, evaluation_time, allowed_lifespan):
    """Return a start time to be used in the partition planning.

    This is a helper method that doesn't always return strictly
    last_changed_time + allowed_lifespan, it prohibits times in the past,
    returning evaluation_time instead, to ensure that we don't try to set
    newly constructed partitions in the past.
    """
    partition_start_time = last_changed_time + allowed_lifespan
    if partition_start_time < evaluation_time:
        # Partition start times should never be in the past.
        return evaluation_time
    return partition_start_time.replace(minute=0, second=0, microsecond=0)


def _get_rate_partitions_with_implicit_timestamps(
    table, filled_partitions, current_position, evaluation_time, active_partition
):
    """ Return a list of PositionPartitions for use in rate calculations.

    The partitions are set with implicit timestamps.
    """
    log = logging.getLogger(
        f"_get_rate_partitions_with_implicit_timestamps:{table.name}"
    )

    rate_relevant_partitions = None

    if active_partition.timestamp() < evaluation_time:
        # This bit of weirdness is a fencepost issue: The partition list is strictly
        # increasing until we get to "now" and the active partition. "Now" actually
        # takes place _after_ active partition's start date (naturally), but
        # contains a position that is before the top of active, by definition. For
        # the rate processing to work, we need to swap the "now" and the active
        # partition's dates and positions.
        rate_relevant_partitions = filled_partitions + [
            partitionmanager.types.InstantPartition(
                "p_current_pos", active_partition.timestamp(), current_position
            ),
            partitionmanager.types.InstantPartition(
                "p_prev_pos", evaluation_time, active_partition.position
            ),
        ]
    else:
        # If the active partition's start date is later than today, then we
        # previously mispredicted the rate of change. There's nothing we can
        # do about that at this point, except limit our rate-of-change calculation
        # to exclude the future-dated, irrelevant partition.
        log.debug(
            f"Misprediction: Evaluation time ({evaluation_time}) is "
            f"before the active partition {active_partition}. Excluding "
            "mispredicted partitions from the rate calculations."
        )
        filled_partitions = filter(
            lambda f: f.timestamp() < evaluation_time, filled_partitions
        )
        rate_relevant_partitions = list(filled_partitions) + [
            partitionmanager.types.InstantPartition(
                "p_current_pos", evaluation_time, current_position
            )
        ]

    return rate_relevant_partitions


def _get_rate_partitions_with_queried_timestamps(
    database, table, partition_list, current_position, evaluation_time, active_partition
):
    """ Return a list of PositionPartitions for use in rate calculations.

    The partitions' timestamps are explicitly queried.
    """
    log = logging.getLogger(
        f"_get_rate_partitions_with_queried_timestamps:{table.name}"
    )

    if not table.has_date_query:
        raise ValueError("Table has no defined date query")

    instant_partitions = list()

    for partition in partition_list:
        if len(partition.position) != 1:
            raise ValueError(
                "This method is only valid for single-column partitions right now"
            )
        arg = partition.position.as_sql_input()[0]

        sql_select_cmd = table.earliest_utc_timestamp_query.get_statement_with_argument(
            arg
        )
        log.debug(
            "Executing %s to derive partition %s at position %s",
            sql_select_cmd,
            partition.name,
            partition.position,
        )

        start = datetime.now()
        exact_time_result = database.run(sql_select_cmd)
        end = datetime.now()

        assert len(exact_time_result) == 1
        assert len(exact_time_result[0]) == 1
        for key, value in exact_time_result[0].items():
            exact_time = datetime.fromtimestamp(value, tz=timezone.utc)
            break

        log.debug(
            "Exact time of %s returned for %s at position %s, query took %s",
            exact_time,
            partition.name,
            partition.position,
            (end - start),
        )

        instant_partitions.append(
            partitionmanager.types.InstantPartition(
                partition.name, exact_time, partition.position
            )
        )

    instant_partitions.append(
        partitionmanager.types.InstantPartition(
            active_partition.name, evaluation_time, current_position
        )
    )

    return instant_partitions


def _plan_partition_changes(
    database,
    table,
    partition_list,
    current_position,
    evaluation_time,
    allowed_lifespan,
    num_empty_partitions,
):
    """Return a list of partitions to modify or create.

    This method makes recommendations in order to meet the supplied table
    requirements, using an estimate as to the rate of fill from the supplied
    partition_list, current_position, and evaluation_time.
    """
    log = logging.getLogger(f"plan_partition_changes:{table.name}")

    filled_partitions, active_partition, empty_partitions = _split_partitions_around_position(
        partition_list, current_position
    )
    if not empty_partitions:
        log.error(
            f"Partition {active_partition.name} requires manual ALTER "
            "as without an empty partition to manipulate, you'll need to "
            "perform an expensive copy operation. See the bootstrap mode."
        )
        raise partitionmanager.types.NoEmptyPartitionsAvailableException()
    if not active_partition:
        raise Exception("Active Partition can't be None")

    if table.has_date_query:
        rate_relevant_partitions = _get_rate_partitions_with_queried_timestamps(
            database,
            table,
            filled_partitions,
            current_position,
            evaluation_time,
            active_partition,
        )
    else:
        rate_relevant_partitions = _get_rate_partitions_with_implicit_timestamps(
            table,
            filled_partitions,
            current_position,
            evaluation_time,
            active_partition,
        )

    rates = _get_weighted_position_increase_per_day_for_partitions(
        rate_relevant_partitions
    )

    log.info(
        f"Rates of change calculated as {rates} per day from "
        f"{len(rate_relevant_partitions)} partitions"
    )

    # We need to include active_partition in the list for the subsequent
    # calculations even though we're not actually changing it.
    results = [partitionmanager.types.ChangePlannedPartition(active_partition)]

    # Adjust each of the empty partitions
    for partition in empty_partitions:
        last_changed = results[-1]

        changed_partition = partitionmanager.types.ChangePlannedPartition(partition)

        start_of_fill_time = _predict_forward_time(
            current_position, last_changed.position, rates, evaluation_time
        )

        if isinstance(partition, partitionmanager.types.PositionPartition):
            # We can't change the position on this partition, but we can adjust
            # the name to be more exact as to what date we expect it to begin
            # filling. If we calculate the start-of-fill date and it doesn't
            # match the partition's name, let's rename it and mark it as an
            # important change.
            if start_of_fill_time.date() != partition.timestamp().date():
                log.info(
                    f"Start-of-fill predicted at {start_of_fill_time.date()} "
                    f"which is not {partition.timestamp().date()}. This change "
                    f"will be marked as important to ensure that {partition} is "
                    f"moved to {start_of_fill_time:%Y-%m-%d}"
                )
                changed_partition.set_timestamp(start_of_fill_time).set_important()

        if isinstance(partition, partitionmanager.types.MaxValuePartition):
            # Only the tail MaxValuePartitions can get new positions. For those,
            # we calculate forward what position we expect and use it in the
            # future.

            nominal_partition_start_time = _calculate_start_time(
                last_changed.timestamp(), evaluation_time, allowed_lifespan
            )

            # We use the nearest timestamp, which should generally be the
            # calculated time, but could be the fill time based on predicting
            # forward if we have gotten far off in our predictions in the past.
            changed_partition.set_timestamp(
                min(nominal_partition_start_time, start_of_fill_time)
            )

            changed_part_pos = _predict_forward_position(
                last_changed.position.as_list(), rates, allowed_lifespan
            )
            changed_partition.set_position(changed_part_pos)

        results.append(changed_partition)

    # Ensure we have the required number of empty partitions
    while len(results) < num_empty_partitions + 1:
        last_changed = results[-1]
        partition_start_time = _calculate_start_time(
            last_changed.timestamp(), evaluation_time, allowed_lifespan
        )

        new_part_pos = _predict_forward_position(
            last_changed.position.as_list(), rates, allowed_lifespan
        )
        results.append(
            partitionmanager.types.NewPlannedPartition()
            .set_position(new_part_pos)
            .set_timestamp(partition_start_time)
        )

    # Confirm we won't make timestamp conflicts
    existing_timestamps = list(map(lambda p: p.timestamp(), partition_list))
    conflict_found = True
    while conflict_found:
        conflict_found = False
        for partition in results:
            if partition.timestamp() in existing_timestamps:
                if (
                    isinstance(partition, partitionmanager.types.ChangePlannedPartition)
                    and partition.timestamp() == partition.old.timestamp()
                ):
                    # That's not a conflict
                    continue

                log.debug(
                    f"{partition} has a conflict for its timestamp, increasing by 1 day."
                )
                partition.set_timestamp(partition.timestamp() + timedelta(days=1))
                conflict_found = True
                break

    # Final result is always MAXVALUE
    results[-1].set_as_max_value()

    log.debug(f"Planned {results}")
    return results


def _should_run_changes(table, altered_partitions):
    """Returns True if the changeset should run, otherwise returns False.

    Evaluate the list from plan_partition_changes and determine if the set of
    changes should be performed - if all the changes are minor, they shouldn't
    be run.
    """
    log = logging.getLogger(f"should_run_changes:{table.name}")

    for p in altered_partitions:
        if isinstance(p, partitionmanager.types.NewPlannedPartition):
            log.debug(f"{p} is new")
            return True

        if isinstance(p, partitionmanager.types.ChangePlannedPartition):
            if p.important():
                log.debug(f"{p} is marked important")
                return True
    return False


def generate_sql_reorganize_partition_commands(table, changes):
    """Generates SQL commands to reorganize table to apply the changes.

    Args:

    table: a types.Table object

    changes: a list of objects implenting types.PlannedPartition
    """
    log = logging.getLogger(f"generate_sql_reorganize_partition_commands:{table.name}")

    modified_partitions = list()
    new_partitions = list()

    for p in changes:
        if isinstance(p, partitionmanager.types.ChangePlannedPartition):
            assert not new_partitions, "Modified partitions must precede new partitions"
            modified_partitions.append(p)
        elif isinstance(p, partitionmanager.types.NewPlannedPartition):
            new_partitions.append(p)
        else:
            raise partitionmanager.types.UnexpectedPartitionException(p)

    # If there's not at least one modification, bail out
    if not new_partitions and not list(
        filter(lambda x: x.has_modifications, modified_partitions)
    ):
        log.debug("No partitions have modifications and no new partitions")
        return

    partition_names_set = set()

    for modified_partition, is_final in reversed(
        list(partitionmanager.tools.iter_show_end(modified_partitions))
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
                raise partitionmanager.types.DuplicatePartitionException(
                    f"Duplicate {part}"
                )
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


def get_pending_sql_reorganize_partition_commands(
    *,
    database,
    table,
    partition_list,
    current_position,
    allowed_lifespan,
    num_empty_partitions,
    evaluation_time,
):
    """Return a list of SQL commands to produce an optimally-partitioned table.

    This algorithm is described in the README.md file as the Maintain Algorithm.

    Args:

    table: The table name and properties

    partition_list: the currently-existing partition objects, each with
        a name and either a starting position or are the tail MAXVALUE.

    current_position: a Position representing the position IDs for
        this table at the evaluation_time.

    allowed_lifespan: a timedelta that represents how long a span of time
        a partition should seek to cover.

    num_empty_partitions: the number of empty partitions to seek to keep at the
        tail, each aiming to span allowed_lifespan.

    evaluation_time: a datetime instance that represents the time the
        algorithm is running.
    """

    log = logging.getLogger(
        f"get_pending_sql_reorganize_partition_commands:{table.name}"
    )

    partition_changes = _plan_partition_changes(
        database,
        table,
        partition_list,
        current_position,
        evaluation_time,
        allowed_lifespan,
        num_empty_partitions,
    )

    if not _should_run_changes(table, partition_changes):
        log.info(f"{table} does not need to be modified currently.")
        return list()

    log.debug(f"{table} has changes waiting.")
    return generate_sql_reorganize_partition_commands(table, partition_changes)

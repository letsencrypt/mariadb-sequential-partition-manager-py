"""
Helper functions for database operations
"""

from datetime import datetime, timezone
import logging

import partitionmanager.table_append_partition as pm_tap
import partitionmanager.types


def get_position_of_table(database, table, map_data):
    """Returns a Position of the table at the current moment."""

    pos_list = pm_tap.get_current_positions(database, table, map_data["range_cols"])

    cur_pos = partitionmanager.types.Position()
    cur_pos.set_position([pos_list[col] for col in map_data["range_cols"]])

    return cur_pos


def calculate_exact_timestamp_via_query(database, table, position_partition):
    """Calculates the exact timestamp of a PositionPartition.

    raises ValueError if the position is incalculable
    """

    log = logging.getLogger(f"calculate_exact_timestamp_via_query:{table.name}")

    if not table.has_date_query:
        raise ValueError("Table has no defined date query")

    if not isinstance(position_partition, partitionmanager.types.PositionPartition):
        raise ValueError("Only PositionPartitions are supported")

    if len(position_partition.position) != 1:
        raise ValueError(
            "This method is only valid for single-column partitions right now"
        )
    arg = position_partition.position.as_sql_input()[0]

    sql_select_cmd = table.earliest_utc_timestamp_query.get_statement_with_argument(arg)
    log.debug(
        "Executing %s to derive partition %s at position %s",
        sql_select_cmd,
        position_partition.name,
        position_partition.position,
    )

    start = datetime.now()
    exact_time_result = database.run(sql_select_cmd)
    end = datetime.now()

    if not len(exact_time_result) == 1:
        raise partitionmanager.types.NoExactTimeException("No exact timestamp result")
    if not len(exact_time_result[0]) == 1:
        raise partitionmanager.types.NoExactTimeException(
            "Unexpected row count for the timestamp result"
        )
    for key, value in exact_time_result[0].items():
        exact_time = datetime.fromtimestamp(value, tz=timezone.utc)
        break

    log.debug(
        "Exact time of %s returned for %s at position %s, query took %s",
        exact_time,
        position_partition.name,
        position_partition.position,
        (end - start),
    )
    return exact_time

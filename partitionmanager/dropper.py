"""
Determine which partitions can be dropped.
"""

import logging

import partitionmanager.types
import partitionmanager.tools


def _drop_statement(table, partition_list):
    """Generate an ALTER TABLE statement to drop these partitions."""

    log = logging.getLogger("get_droppable_partitions")

    partitions = ",".join(map(lambda x: f"`{x.name}`", partition_list))

    alter_cmd = (
        f"ALTER TABLE `{table.name}` " f"DROP PARTITION IF EXISTS {partitions} ;"
    )

    log.debug("Yielding %s", alter_cmd)

    return alter_cmd


def get_droppable_partitions(
    database, partitions, current_position, current_timestamp, table
):
    """Return a dictionary of partitions which can be dropped and why."""
    log = logging.getLogger("get_droppable_partitions")
    results = {}
    droppable = []

    if not table.retention_period:
        raise ValueError(f"{table.name} does not have a retention period set")

    if not partitions:
        return results

    for partition, next_partition in partitionmanager.tools.pairwise(partitions):
        if next_partition >= current_position:
            log.debug(
                "Stopping at %s because current position %s indicates "
                "subsequent partition is empty",
                partition,
                current_position,
            )
            break

        if isinstance(next_partition, partitionmanager.types.MaxValuePartition):
            log.debug("Stopping at %s because we can't handle MaxValuePartitions.")
            break

        assert isinstance(next_partition, partitionmanager.types.PositionPartition)

        approx_size = 0
        for a, b in zip(
            next_partition.position.as_list(), partition.position.as_list()
        ):
            approx_size += a - b

        try:
            start_time = (
                partitionmanager.database_helpers.calculate_exact_timestamp_via_query(
                    database, table, partition
                )
            )
            end_time = (
                partitionmanager.database_helpers.calculate_exact_timestamp_via_query(
                    database, table, next_partition
                )
            )

            oldest_age = current_timestamp - start_time
            youngest_age = current_timestamp - end_time

            if youngest_age > table.retention_period:
                results[partition.name] = {
                    "oldest_time": f"{start_time}",
                    "youngest_time": f"{end_time}",
                    "oldest_position": partition.position,
                    "youngest_position": next_partition.position,
                    "oldest_age": f"{oldest_age}",
                    "youngest_age": f"{youngest_age}",
                    "approx_size": approx_size,
                }
                droppable.append(partition)
        except partitionmanager.types.NoExactTimeException:
            log.warning(
                "Couldn't determine exact times for %s.%s, it is probably droppable too.",
                table,
                partition,
            )

            results[partition.name] = {
                "oldest_time": "unable to determine",
                "youngest_time": "unable to determine",
                "oldest_position": partition.position,
                "youngest_position": next_partition.position,
                "oldest_age": "unable to determine",
                "youngest_age": "unable to determine",
                "approx_size": approx_size,
            }
            droppable.append(partition)

    if droppable:
        results["drop_query"] = _drop_statement(table, droppable)

    return results

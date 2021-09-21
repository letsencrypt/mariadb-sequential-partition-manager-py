"""
Bootstrap a table that does not have sufficient partitions to determine rates
of change.
"""

from datetime import timedelta
import logging
import operator
import yaml

import partitionmanager.table_append_partition as pm_tap
import partitionmanager.tools
import partitionmanager.types

RATE_UNIT = timedelta(hours=1)
MINIMUM_FUTURE_DELTA = timedelta(hours=2)


def _override_config_to_map_data(conf):
    """Return an analog to get_partition_map from override data in conf"""
    return {
        "range_cols": [str(x) for x in conf.assume_partitioned_on],
        "partitions": [
            partitionmanager.types.MaxValuePartition(
                "p_assumed", count=len(conf.assume_partitioned_on)
            )
        ],
    }


def _get_map_data_from_config(conf, table):
    """ Helper to return a partition map for the table, either directly or
        from a configuration override. """
    if not conf.assume_partitioned_on:
        problems = pm_tap.get_table_compatibility_problems(conf.dbcmd, table)
        if problems:
            raise Exception("; ".join(problems))
        return pm_tap.get_partition_map(conf.dbcmd, table)

    return _override_config_to_map_data(conf)


def write_state_info(conf, out_fp):
    """
    Write the state info for tables defined in conf to the provided file-like
    object.
    """
    log = logging.getLogger("write_state_info")

    log.info("Writing current state information")
    state_info = {"time": conf.curtime, "tables": dict()}
    for table in conf.tables:
        map_data = _get_map_data_from_config(conf, table)

        positions = pm_tap.get_current_positions(
            conf.dbcmd, table, map_data["range_cols"]
        )

        log.info(f'(Table("{table.name}"): {positions}),')
        state_info["tables"][str(table.name)] = positions

    yaml.dump(state_info, out_fp)


def _get_time_offsets(num_entries, first_delta, subseq_delta):
    """
    Construct a list of timedeltas of size num_entries of the form
    [ first_delta, subseq_delta, [subseq_delta...] ]
    """
    if num_entries < 1:
        raise ValueError("Must request at least one entry")

    time_units = [first_delta]
    while len(time_units) < num_entries:
        prev = time_units[-1]
        time_units.append(prev + subseq_delta)
    return time_units


def _plan_partitions_for_time_offsets(
    now_time, time_offsets, rate_of_change, ordered_current_pos, max_val_part
):
    """
    Return a list of PlannedPartitions whose positions are predicted to
    lie upon the supplied time_offsets, given the initial conditions supplied
    in the other parameters.

    types:
        time_offsets: an ordered list of timedeltas to plan to reach

        rate_of_change: an ordered list of positions per RATE_UNIT.
    """
    changes = list()
    for (i, offset), is_final in partitionmanager.tools.iter_show_end(
        enumerate(time_offsets)
    ):
        increase = [x * (offset / RATE_UNIT) for x in rate_of_change]
        predicted_positions = [
            int(p + i) for p, i in zip(ordered_current_pos, increase)
        ]
        predicted_time = now_time + offset

        part = None
        if i == 0:
            part = (
                partitionmanager.types.ChangePlannedPartition(max_val_part)
                .set_position(predicted_positions)
                .set_timestamp(predicted_time)
            )

        else:
            part = partitionmanager.types.NewPlannedPartition().set_timestamp(
                predicted_time
            )

            if is_final:
                part.set_columns(len(predicted_positions))
            else:
                part.set_position(predicted_positions)

        changes.append(part)
    return changes


def _suffix(lines, *, indent="", mid_suffix="", final_suffix=""):
    """ Helper that suffixes each line with either mid- or final- suffix """
    for line, is_final in partitionmanager.tools.iter_show_end(lines):
        if is_final:
            yield indent + line + final_suffix
        else:
            yield indent + line + mid_suffix


def _trigger_column_copies(cols):
    """ Helper that returns lines copying each column for a trigger. """
    for c in cols:
        yield f"`{c}` = NEW.`{c}`"


def _generate_sql_copy_commands(
    existing_table, map_data, columns, new_table, alter_commands_iter
):
    """ Generate a series of SQL commands to start a copy of the existing_table
    to a new_table, applying the supplied alterations before starting the
    triggers. """
    log = logging.getLogger(
        f"_generate_sql_copy_commands:{existing_table.name} to {new_table.name}"
    )

    max_val_part = map_data["partitions"][-1]
    if not isinstance(max_val_part, partitionmanager.types.MaxValuePartition):
        msg = f"Expected a MaxValue partition, got {max_val_part}"
        log.error(msg)
        raise Exception(msg)

    range_id_string = ", ".join(map_data["range_cols"])

    if len(map_data["range_cols"]) == 1:
        range_cols_string = "RANGE"
        max_val_string = "MAXVALUE"
    else:
        num_cols = len(map_data["range_cols"])
        range_cols_string = "RANGE COLUMNS"
        max_val_string = "(" + ", ".join(["MAXVALUE"] * num_cols) + ")"

    yield f"DROP TABLE IF EXISTS {new_table.name};"
    yield f"CREATE TABLE {new_table.name} LIKE {existing_table.name};"
    yield f"ALTER TABLE {new_table.name} REMOVE PARTITIONING;"
    yield f"ALTER TABLE {new_table.name} PARTITION BY {range_cols_string} ({range_id_string}) ("
    yield f"\tPARTITION {max_val_part.name} VALUES LESS THAN {max_val_string}"
    yield ");"

    for command in alter_commands_iter:
        yield command

    cols = set(columns)

    yield f"CREATE OR REPLACE TRIGGER copy_inserts_from_{existing_table.name}_to_{new_table.name}"
    yield f"\tAFTER INSERT ON {existing_table.name} FOR EACH ROW"
    yield f"\t\tINSERT INTO {new_table.name} SET"

    for line in _suffix(
        _trigger_column_copies(sorted(cols)),
        indent="\t\t\t",
        mid_suffix=",",
        final_suffix=";",
    ):
        yield line

    update_columns = cols.difference(set(map_data["range_cols"]))
    if not update_columns:
        log.info("No columns to copy, so no UPDATE trigger being constructed.")
        return

    yield f"CREATE OR REPLACE TRIGGER copy_updates_from_{existing_table.name}_to_{new_table.name}"
    yield f"\tAFTER UPDATE ON {existing_table.name} FOR EACH ROW"
    yield f"\t\tUPDATE {new_table.name} SET"

    for line in _suffix(
        _trigger_column_copies(sorted(update_columns)), indent="\t\t\t", mid_suffix=","
    ):
        yield line

    yield "\t\tWHERE " + " AND ".join(
        _trigger_column_copies(map_data["range_cols"])
    ) + ";"

    return


def calculate_sql_alters_from_state_info(conf, in_fp):
    """
    Using the config and the input yaml file-like object, return the SQL
    statements to bootstrap the tables in config that also have data in
    the input yaml as a dictionary of { Table -> list(SQL ALTER statements) }
    """
    log = logging.getLogger("calculate_sql_alters")

    log.info("Reading prior state information")
    prior_data = yaml.safe_load(in_fp)

    time_delta = (conf.curtime - prior_data["time"]) / RATE_UNIT
    if time_delta <= 0:
        raise ValueError(
            f"Time delta is too small: {conf.curtime} - "
            f"{prior_data['time']} = {time_delta}"
        )

    commands = dict()

    for table_name, prior_pos in prior_data["tables"].items():
        table = None
        for t in conf.tables:
            if t.name == table_name:
                table = t
        if not table:
            log.info(f"Skipping {table_name} as it is not in the current config")
            continue

        map_data = _get_map_data_from_config(conf, table)

        current_positions = pm_tap.get_current_positions(
            conf.dbcmd, table, map_data["range_cols"]
        )

        columns = [r["Field"] for r in pm_tap.get_columns(conf.dbcmd, table)]

        ordered_current_pos = [
            current_positions[name] for name in map_data["range_cols"]
        ]
        ordered_prior_pos = [prior_pos[name] for name in map_data["range_cols"]]

        delta_positions = list(
            map(operator.sub, ordered_current_pos, ordered_prior_pos)
        )
        rate_of_change = list(map(lambda pos: pos / time_delta, delta_positions))

        max_val_part = map_data["partitions"][-1]
        if not isinstance(max_val_part, partitionmanager.types.MaxValuePartition):
            log.error(f"Expected a MaxValue partition, got {max_val_part}")
            raise Exception("Unexpected part?")

        log.info(
            f"{table}, {time_delta:0.1f} hours, {ordered_prior_pos} - {ordered_current_pos}, "
            f"{delta_positions} pos_change, {rate_of_change}/hour"
        )

        part_duration = conf.partition_period
        if table.partition_period:
            part_duration = table.partition_period

        # Choose the times for each partition that we are configured to
        # construct, beginning in the near future (see MINIMUM_FUTURE_DELTA),
        # to provide a quick changeover into the new partition schema.
        time_offsets = _get_time_offsets(
            1 + conf.num_empty, MINIMUM_FUTURE_DELTA, part_duration
        )

        changes = _plan_partitions_for_time_offsets(
            conf.curtime,
            time_offsets,
            rate_of_change,
            ordered_current_pos,
            max_val_part,
        )

        table_new = partitionmanager.types.Table(
            f"{table.name}_new_{conf.curtime:%Y%m%d}"
        )

        alter_commands_iter = pm_tap.generate_sql_reorganize_partition_commands(
            table_new, changes
        )

        commands[table.name] = list(
            _generate_sql_copy_commands(
                table, map_data, columns, table_new, alter_commands_iter
            )
        )
    return commands

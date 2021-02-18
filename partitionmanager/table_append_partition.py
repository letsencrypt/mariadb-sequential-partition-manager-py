from partitionmanager.types import (
    DuplicatePartitionException,
    MismatchedIdException,
    SqlInput,
    TableInformationException,
    UnexpectedPartitionException,
)
from datetime import timezone, datetime
import logging
import re


def assert_table_is_compatible(database, table_name):
    """
    Gather the information schema from the database command and parse out the
    autoincrement value.
    """
    db_name = database.db_name()

    if type(db_name) != SqlInput or type(table_name) != SqlInput:
        raise ValueError("Unexpected type")
    sql_cmd = (
        "SELECT CREATE_OPTIONS FROM INFORMATION_SCHEMA.TABLES "
        + f"WHERE TABLE_SCHEMA='{db_name}' and TABLE_NAME='{table_name}';"
    ).strip()

    assert_table_information_schema_compatible(database.run(sql_cmd), table_name)


def assert_table_information_schema_compatible(rows, table_name):
    """
    Parse a table information schema, validating options
    """
    if len(rows) != 1:
        raise TableInformationException(f"Unable to read information for {table_name}")

    options = rows[0]
    if "partitioned" not in options["CREATE_OPTIONS"]:
        raise TableInformationException(f"Table {table_name} is not partitioned")


def get_current_positions(database, table_name, columns):
    """
    Get the positions of the columns provided in the given table, return
    as a list in the same order as the provided columns
    """
    if type(columns) is not list:
        raise ValueError("columns must be a list")

    order_col = columns[0]
    columns_str = ", ".join([f"`{x}`" for x in columns])
    sql = f"SELECT {columns_str} FROM `{table_name}` ORDER BY {order_col} DESC LIMIT 1;"
    rows = database.run(sql)
    if len(rows) != 1:
        raise TableInformationException("Expected one result")
    ordered_positions = list()
    for c in columns:
        ordered_positions.append(rows[0][c])
    return ordered_positions


def get_partition_map(database, table_name):
    """
    Gather the partition map via the database command tool.
    """
    if type(table_name) != SqlInput:
        raise ValueError("Unexpected type")
    sql_cmd = f"SHOW CREATE TABLE `{table_name}`;".strip()
    return parse_partition_map(database.run(sql_cmd))


def parse_partition_map(rows):
    """
    Read a partition statement from a table creation string and produce tuples
    for each partition with a max, and a single string for the partition using
    "maxvalue".
    """
    partition_range = re.compile(r"[ ]*PARTITION BY RANGE \(([\w,` ]+)\)")
    partition_member = re.compile(
        r"[ (]*PARTITION `(\w+)` VALUES LESS THAN \(([\d, ]+)\)"
    )
    partition_tail = re.compile(
        r"[ (]*PARTITION `(\w+)` VALUES LESS THAN \(?(MAXVALUE[, ]*)+\)?"
    )

    range_cols = None
    partitions = list()

    if len(rows) != 1:
        raise TableInformationException("Expected one result")

    options = rows[0]

    for l in options["Create Table"].split("\n"):
        range_match = partition_range.match(l)
        if range_match:
            range_cols = [x.strip("` ") for x in range_match.group(1).split(",")]
            logging.debug(f"Partition range columns: {range_cols}")

        member_match = partition_member.match(l)
        if member_match:
            part_name, part_vals_str = member_match.group(1, 2)
            logging.debug(f"Found partition {part_name} = {part_vals_str}")

            part_vals = [int(x.strip("` ")) for x in part_vals_str.split(",")]

            if len(part_vals) != len(range_cols):
                logging.error(
                    f"Partition columns {part_vals} don't match the partition range {range_cols}"
                )
                raise MismatchedIdException("Partition columns mismatch")

            partitions.append((part_name, part_vals))

        member_tail = partition_tail.match(l)
        if member_tail:
            part_name = member_tail.group(1)
            logging.debug(f"Found tail partition named {part_name}")
            partitions.append(part_name)

    return {"range_cols": range_cols, "partitions": partitions}


def parition_name_now():
    """
    Format a partition name for now
    """
    return datetime.now(tz=timezone.utc).strftime("p_%Y%m%d")


def reorganize_partition(partition_list, new_partition_name, partition_positions):
    """
    From a partial partitions list (ending with a single value that indicates MAX VALUE),
    add a new partition at the partition_positions, which must be a list.
    """
    if type(partition_positions) is not list:
        raise ValueError()

    last_value = partition_list.pop()
    if type(last_value) is not str:
        raise UnexpectedPartitionException(last_value)
    if last_value == new_partition_name:
        raise DuplicatePartitionException(last_value)
    if type(partition_list[0][1]) is list:
        if len(partition_list[0][1]) != len(partition_positions):
            raise MismatchedIdException("Didn't get the same number of partition IDs")
    else:
        if len(partition_positions) != 1:
            raise MismatchedIdException("Expected only a single partition ID")

    positions_str = ", ".join([str(x) for x in partition_positions])
    maxvalue_str = ", ".join(["MAXVALUE"] * len(partition_positions))

    reorganized_list = list()
    reorganized_list.append((last_value, f"({positions_str})"))
    reorganized_list.append((new_partition_name, maxvalue_str))
    return last_value, reorganized_list


def format_sql_reorganize_partition_command(
    table_name, *, partition_to_alter, partition_list
):
    """
    Produce a SQL command to reorganize the partition in table_name to
    match the new partition_list.
    """
    partition_strings = list()
    for p in partition_list:
        if type(p) is not tuple:
            raise UnexpectedPartitionException(p)
        partition_strings.append(f"PARTITION `{p[0]}` VALUES LESS THAN {p[1]}")
    partition_update = ", ".join(partition_strings)

    return (
        f"ALTER TABLE `{table_name}` "
        f"REORGANIZE PARTITION `{partition_to_alter}` INTO ({partition_update});"
    )

from partitionmanager.types import (
    MismatchedIdException,
    SqlInput,
    TableInformationException,
    UnexpectedPartitionException,
)
from datetime import timezone, datetime
import logging
import re


def get_autoincrement(database, db_name, table_name):
    """
    Gather the information schema from the database command and parse out the
    autoincrement value.
    """
    if type(db_name) != SqlInput or type(table_name) != SqlInput:
        raise ValueError("Unexpected type")
    sql_cmd = f"""
               SELECT AUTO_INCREMENT, CREATE_OPTIONS FROM
                  INFORMATION_SCHEMA.TABLES
               WHERE
                  TABLE_SCHEMA=`{db_name}` and TABLE_NAME=`{table_name}`;"
               """.strip()

    return parse_table_information_schema(database.run(sql_cmd))


def parse_table_information_schema(text):
    """
    Parse a table information schema, validating options and returning the
    current autoincrement ID.
    """
    options = dict()
    for l in text.split("\n"):
        if ": " in l:
            k, v = l.split(": ")
            options[k] = v

    if options["AUTO_INCREMENT"] in ("null", "NULL"):
        raise TableInformationException(
            f"Auto Increment value is {options['AUTO_INCREMENT']}"
        )

    if "partitioned" not in options["CREATE_OPTIONS"]:
        raise TableInformationException(
            f"Partitioned is not in the features: {options['CREATE_OPTIONS']}"
        )

    try:
        return int(options["AUTO_INCREMENT"])
    except ValueError as ve:
        raise TableInformationException(
            f"Auto Increment value cannot be cast to an int: {ve}"
        )


def get_partition_map(database, db_name, table_name):
    """
    Gather the partition map via the database command tool.
    """
    if type(db_name) != SqlInput or type(table_name) != SqlInput:
        raise ValueError("Unexpected type")
    sql_cmd = f"SHOW CREATE TABLE `{db_name}`.`{table_name}`;".strip()
    return parse_partition_map(database.run(sql_cmd))


def parse_partition_map(text):
    """
    Read a partition statement from a table creation string and produce tuples
    for each partition with a max, and a single string for the partition using
    "maxvalue".
    """
    auto_increment = re.compile(r" *`(\w+)` .* AUTO_INCREMENT.*,")
    partition_range = re.compile(r" PARTITION BY RANGE \(`(\w+)`\)")
    partition_member = re.compile(r"[ (]*PARTITION `(\w+)` VALUES LESS THAN (\(\d+\))")
    partition_tail = re.compile(r"[ (]*PARTITION `(\w+)` VALUES LESS THAN MAXVALUE")

    ai_col = None
    range_col = None
    partitions = list()

    for l in text.split("\n"):
        ai_match = auto_increment.match(l)
        if ai_match:
            ai_col = ai_match.group(1)
            logging.debug(f"Auto_Increment column identified as {ai_col}")

        range_match = partition_range.match(l)
        if range_match:
            range_col = range_match.group(1)
            logging.debug(f"Partition range column identified as {range_col}")

        member_match = partition_member.match(l)
        if member_match:
            t = member_match.group(1, 2)
            logging.debug(f"Found partition {t[0]} = {t[1]}")
            partitions.append(t)

        member_tail = partition_tail.match(l)
        if member_tail:
            t = member_tail.group(1)
            logging.debug(f"Found tail partition named {t}")
            partitions.append(t)

    if ai_col != range_col:
        logging.error(
            f"Auto_Increment column {ai_col} doesn't match the partition range {range_col}"
        )
        raise MismatchedIdException("Partition ID mismatch")

    return partitions


def parition_name_now():
    """
    Format a partition name for now
    """
    return datetime.now(tz=timezone.utc).strftime("p_%Y%m%d")


def reorganize_partition(partition_list, auto_increment):
    """
    From a partial partitions list (ending with a single value that indicates MAX VALUE),
    add a new partition at the auto_increment number.
    """
    last_value = partition_list.pop()
    if type(last_value) is not str:
        raise UnexpectedPartitionException(last_value)
    reorganized_list = list()
    reorganized_list.append((last_value, f"({auto_increment})"))
    reorganized_list.append((parition_name_now(), "MAXVALUE"))
    return last_value, reorganized_list


def format_sql_reorganize_partition_command(
    db_name, table_name, *, partition_to_alter, partition_list
):
    """
    Produce a SQL command to reorganize the partition in db_name.table_name to
    match the new partition_list.
    """
    partition_strings = list()
    for p in partition_list:
        if type(p) is not tuple:
            raise UnexpectedPartitionException(p)
        partition_strings.append(f"PARTITION `{p[0]}` VALUES LESS THAN {p[1]}")
    partition_update = ", ".join(partition_strings)

    return (
        f"ALTER TABLE `{db_name}`.`{table_name}` "
        f"REORGANIZE PARTITION `{partition_to_alter}` INTO ({partition_update});"
    )

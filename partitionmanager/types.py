"""
Classes and types used across the Partition Manager
"""

import abc
import argparse
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse


def timedelta_from_dict(r):
    """
    Process a dictionary, typically from YAML, which describes a table's
    retention or partition period. Returns a timedelta or None, and raises an argparse
    error if the arguments are not understood.
    """
    for k, v in r.items():
        if k == "days":
            return timedelta(days=v)
        raise argparse.ArgumentTypeError(
            f"Unknown retention period definition: {k}={v}"
        )


class Table:
    """
    Represents enough information about a table to make partitioning decisions.
    """

    def __init__(self, name):
        self.name = SqlInput(name)
        self.retention_period = None
        self.partition_period = None
        self.earliest_utc_timestamp_query = None

    def set_retention_period(self, ret):
        """
        Sets the retention period as a timedelta for this table
        """
        if not isinstance(ret, timedelta):
            raise ValueError("Must be a timedelta")
        self.retention_period = ret
        return self

    def set_partition_period(self, dur):
        """
        Sets the partition period as a timedelta for this table
        """
        if not isinstance(dur, timedelta):
            raise ValueError("Must be a timedelta")
        self.partition_period = dur
        return self

    def set_earliest_utc_timestamp_query(self, query):
        if not isinstance(query, SqlQuery):
            raise ValueError("Must be a SqlQuery")
        self.earliest_utc_timestamp_query = query

    @property
    def has_date_query(self):
        return self.earliest_utc_timestamp_query is not None

    def __str__(self):
        return f"Table {self.name}"


class SqlInput(str):
    """
    Class which wraps a string or number only if it is safe to use within a
    single SQL statement.
    """

    valid_form = re.compile(r"^[A-Z0-9_-]+$", re.IGNORECASE)

    def __new__(cls, *args):
        if len(args) != 1:
            raise argparse.ArgumentTypeError(f"{args} is not a single argument")
        if not isinstance(args[0], int) and not SqlInput.valid_form.match(args[0]):
            raise argparse.ArgumentTypeError(f"{args[0]} is not a valid SQL identifier")
        return super().__new__(cls, args[0])

    def __repr__(self):
        return str(self)


class SqlQuery(str):
    """
    Class which loosely enforces that there's a single SQL SELECT statement to run.
    """

    forbidden_terms = ["UPDATE ", "INSERT ", "DELETE "]

    def __new__(cls, *args):
        if len(args) != 1:
            raise argparse.ArgumentTypeError(f"{args} is not a single argument")
        query_string = args[0].strip()
        if not query_string.endswith(";"):
            raise argparse.ArgumentTypeError(
                f"[{query_string}] does not end with a ';'"
            )
        if query_string.count(";") > 1:
            raise argparse.ArgumentTypeError(
                f"[{query_string}] has more than one statement"
            )

        if "?" not in query_string:
            raise argparse.ArgumentTypeError(
                f"[{query_string}] has no substitution variable '?'"
            )
        if query_string.count("?") > 1:
            raise argparse.ArgumentTypeError(
                f"[{query_string}] has more than one substitution variable '?'"
            )

        if not query_string.upper().startswith("SELECT "):
            raise argparse.ArgumentTypeError(
                f"[{query_string}] is not a SELECT statement"
            )
        for term in SqlQuery.forbidden_terms:
            if term in query_string.upper():
                raise argparse.ArgumentTypeError(
                    f"[{query_string}] has a forbidden term [{term}]"
                )

        return super().__new__(cls, query_string)

    def __repr__(self):
        return str(self)

    def get_statement_with_argument(self, arg):
        if not isinstance(arg, SqlInput):
            raise argparse.ArgumentTypeError("Must be a SqlInput")
        return str(self).replace("?", str(arg))


def to_sql_url(urlstring):
    """
    Parse a sql://user:pass@host:port/schema URL and return the tuple.
    """
    try:
        urltuple = urlparse(urlstring)
        if urltuple.scheme.lower() != "sql":
            raise argparse.ArgumentTypeError(f"{urlstring} is not a valid sql://")
        if urltuple.path == "/" or urltuple.path == "":
            raise argparse.ArgumentTypeError(f"{urlstring} should include a db path")
        return urltuple
    except ValueError as ve:
        raise argparse.ArgumentTypeError(f"{urlstring} not valid: {ve}")


class DatabaseCommand(abc.ABC):
    """
    Abstract class which can run SQL commands and return the results in a
    minimal form.
    """

    @abc.abstractmethod
    def run(self, sql_cmd):
        """
        Run the sql, returning the results as a list of python-ized types, or
        raising an Exception
        """

    @abc.abstractmethod
    def db_name(self):
        """
        Return the current database name
        """


def is_partition_type(obj):
    """True if the object inherits from a _Partition."""
    return isinstance(obj, _Partition)


class _Partition(abc.ABC):
    """Abstract class which represents a existing table partition."""

    @abc.abstractmethod
    def values(self):
        """Return a SQL partition value string."""

    @property
    @abc.abstractmethod
    def name(self):
        """Name representing when the partition began to fill.

        Generally this will be of the form p_yyyymmdd, but sometimes partitions
        have names like p_initial, p_start, or any other valid SQL identifier.
        """

    @property
    @abc.abstractmethod
    def num_columns(self):
        """Return the number of columns included in this partition's range."""

    @property
    def has_real_time(self):
        """True if the partition has a non-synthetic timestamp.

        This should be used to determine whether timestamp() should be used for
        statistical purposes, as timestamp() generates a synthetic timestamp
        for rate-of-change calculations in corner-cases.
        """
        if "p_start" in self.name or not self.name.startswith("p_"):
            return False
        return self.timestamp() is not None

    def timestamp(self):
        """Returns datetime of this partition's date, or None.

        This returns the date from the partition's name if the partition is of
        the form "p_YYYYMMDD". If the name is "p_start", return a synthetic
        timestamp (be sure to use self.has_real_time before using for
        statistical purposes). Otherwise, returns None.
        """

        if not self.name.startswith("p_"):
            return None

        if "p_start" in self.name:
            # Gotta start somewhere, for partitions named things like
            # "p_start". This has the downside of causing abnormally-low
            # rate of change calculations, but they fall off quickly
            # for subsequent partitions
            return datetime(2021, 1, 1, tzinfo=timezone.utc)

        try:
            return datetime.strptime(self.name, "p_%Y%m%d").replace(tzinfo=timezone.utc)
        except ValueError:
            pass
        try:
            return datetime.strptime(self.name, "p_%Y%m").replace(tzinfo=timezone.utc)
        except ValueError:
            pass
        try:
            return datetime.strptime(self.name, "p_%Y").replace(tzinfo=timezone.utc)
        except ValueError:
            pass
        return None

    def __repr__(self):
        return f"{type(self).__name__}<{str(self)}>"

    def __str__(self):
        return f"{self.name}: {self.values()}"


class Position:
    """An internal class that represents a position as an ordered list of
    identifiers, matching the table's partition-by statement.
    """

    def __init__(self):
        self._position = list()

    def set_position(self, position_in):
        """Set the list of identifiers for this position."""
        if isinstance(position_in, Position):
            self._position = position_in.as_list()
        elif isinstance(position_in, list) or isinstance(position_in, tuple):
            self._position = [int(p) for p in position_in]
        else:
            raise ValueError(f"Unexpected position input: {position_in}")
        return self

    def as_list(self):
        """Return a copy of the list of identifiers representing this position"""
        return self._position.copy()

    def as_sql_input(self):
        """Return the position as an array of SqlInput objects"""
        return [SqlInput(p) for p in self._position]

    def __len__(self):
        return len(self._position)

    def __eq__(self, other):
        if isinstance(other, Position):
            return self._position == other.as_list()
        return False

    def __str__(self):
        return str(self._position)

    def __repr__(self):
        return repr(self._position)


class PositionPartition(_Partition):
    """A partition that has a position associated with it.

    Partitions are independent table segments, and each has a name and a current
    position. The positions-list is an ordered list of identifiers, matching
    the order of the table's partition-by statement when the table was created.
    """

    def __init__(self, name):
        self._name = name
        self._position = Position()

    @property
    def name(self):
        return self._name

    def set_position(self, position_in):
        """Set the position for this partition."""
        self._position.set_position(position_in)
        return self

    @property
    def position(self):
        """Return the Position this partition represents"""
        return self._position

    @property
    def num_columns(self):
        return len(self._position)

    def values(self):
        return "(" + ", ".join([str(x) for x in self._position.as_list()]) + ")"

    def __lt__(self, other):
        if isinstance(other, MaxValuePartition):
            if len(self._position) != other.num_columns:
                raise UnexpectedPartitionException(
                    f"Expected {len(self._position)} columns but "
                    f"partition has {other.num_columns}."
                )
            return True

        other_position_list = None
        if isinstance(other, list):
            other_position_list = other
        elif isinstance(other, Position):
            other_position_list = other.as_list()
        elif isinstance(other, PositionPartition):
            other_position_list = other.position.as_list()

        if not other_position_list or len(self._position) != len(other_position_list):
            raise UnexpectedPartitionException(
                f"Expected {len(self._position)} columns but partition has {other_position_list}."
            )

        # If ALL of v_mine >= v_other, then self is greater than other
        # If ANY of v_mine < v_other, then self is less than other
        for v_mine, v_other in zip(self._position.as_list(), other_position_list):
            if v_mine < v_other:
                return True
        return False

    def __ge__(self, other):
        return not self < other

    def __eq__(self, other):
        if isinstance(other, PositionPartition):
            return self.name == other.name and self._position == other.position
        elif isinstance(other, MaxValuePartition):
            return False

        raise ValueError(f"Unexpected equality with {other}")


class MaxValuePartition(_Partition):
    """A partition that includes all remaining values.

    This kind of partition always resides at the tail of the partition list,
    and is defined as containing values up to the reserved keyword MAXVALUE.
    """

    def __init__(self, name, count):
        self._name = name
        self._count = count

    @property
    def name(self):
        return self._name

    @property
    def num_columns(self):
        return self._count

    def values(self):
        if self._count == 1:
            return "MAXVALUE"
        return "(" + ", ".join(["MAXVALUE"] * self._count) + ")"

    def __lt__(self, other):
        """MaxValuePartitions are always greater than every other partition."""
        if isinstance(other, list) or isinstance(other, Position):
            if self._count != len(other):
                raise UnexpectedPartitionException(
                    f"Expected {self._count} columns but list has {len(other)}."
                )
            return False
        if is_partition_type(other):
            if self._count != other.num_columns:
                raise UnexpectedPartitionException(
                    f"Expected {self._count} columns but list has {other.num_columns}."
                )
            return False
        return ValueError()

    def __ge__(self, other):
        return not self < other

    def __eq__(self, other):
        if isinstance(other, MaxValuePartition):
            return self.name == other.name and self._count == other.num_columns
        elif isinstance(other, PositionPartition):
            return False
        raise ValueError(f"Unexpected equality with {other}")


class InstantPartition(PositionPartition):
    """Represent a partition at the current moment.

    Used for rate calculations as a stand-in that only exists for the purposes
    of the rate calculation itself.
    """

    def __init__(self, name, now, position_in):
        super().__init__(name)
        self._instant = now
        self._position.set_position(position_in)

    def timestamp(self):
        return self._instant


class _PlannedPartition(abc.ABC):
    """Represents a partition this tool plans to emit.

    The method as_partition will make this a concrete type for later evaluation.
    """

    def __init__(self):
        self._num_columns = None
        self._position = None
        self._timestamp = None
        self._important = False

    def set_timestamp(self, timestamp):
        """Set the timestamp to be used for the modified partition.

        This effectively changes the partition's name.
        """
        self._timestamp = timestamp.replace(hour=0, minute=0)
        return self

    def set_position(self, position_in):
        """Set the position of this modified partition.

        If this partition changes an existing partition, the positions of both
        must have identical length.
        """
        pos = Position()
        pos.set_position(position_in)

        if self.num_columns is not None and len(pos) != self.num_columns:
            raise UnexpectedPartitionException(
                f"Expected {self.num_columns} columns but input has {len(pos)}."
            )

        self._position = pos
        return self

    def set_important(self):
        """Indicate this is an important partition. Used in the
        _plan_partition_changes as a marker that there's a significant
        change in this partition that should be committed even if the
        overall map isn't changing much."""
        self._important = True
        return self

    @property
    def position(self):
        """Get the position for this modified partition."""
        return self._position

    def timestamp(self):
        """The timestamp of this partition."""
        return self._timestamp

    def important(self):
        """True if this Partition is important enough to ensure commitment."""
        return self._important

    @property
    @abc.abstractmethod
    def has_modifications(self):
        """True if this partition modifies another partition."""

    @property
    def num_columns(self):
        """Return the number of columns this partition represents."""
        return self._num_columns

    def set_as_max_value(self):
        """Represent this partition by MaxValuePartition from as_partition()"""
        self._num_columns = len(self._position)
        self._position = None
        return self

    def as_partition(self):
        """Return a concrete Partition that can be rendered into a SQL ALTER."""
        if not self._timestamp:
            raise ValueError()
        if self._position:
            return PositionPartition(f"p_{self._timestamp:%Y%m%d}").set_position(
                self._position
            )
        return MaxValuePartition(f"p_{self._timestamp:%Y%m%d}", count=self._num_columns)

    def __repr__(self):
        return f"{type(self).__name__}<{str(self)}>"

    def __eq__(self, other):
        if isinstance(other, _PlannedPartition):
            return (
                isinstance(self, type(other))
                and self.position == other.position
                and self.timestamp() == other.timestamp()
                and self.important() == other.important()
            )
        return False


class ChangePlannedPartition(_PlannedPartition):
    """Represents modifications to a Partition supplied during construction.

    Use the parent class' methods to alter this change.
    """

    def __init__(self, old_part):
        if not is_partition_type(old_part):
            raise ValueError()
        super().__init__()
        self._old = old_part
        self._num_columns = self._old.num_columns
        self._timestamp = self._old.timestamp()
        self._old_position = (
            self._old.position if isinstance(old_part, PositionPartition) else None
        )
        self._position = self._old_position

    @property
    def has_modifications(self):
        return (
            self._position != self._old_position
            or self._old.timestamp() is None
            and self._timestamp is not None
            or self._timestamp.date() != self._old.timestamp().date()
        )

    @property
    def old(self):
        """Get the partition to be modified"""
        return self._old

    def __str__(self):
        imp = "[!!]" if self.important() else ""
        return f"{self._old} => {self.position} {imp} {self._timestamp}"


class NewPlannedPartition(_PlannedPartition):
    """Represents a wholly new Partition to be constructed.

    After construction, you must set the number of columns using set_columns
    before attempting to use this in a plan.
    """

    def __init__(self):
        super().__init__()
        self.set_important()

    def set_columns(self, count):
        """Set the number of columns needed to represent a position for this
        partition."""
        self._num_columns = count
        return self

    @property
    def has_modifications(self):
        return False

    def __str__(self):
        return f"Add: {self.position} {self._timestamp}"


class MismatchedIdException(Exception):
    """Raised if the partition map doesn't use the primary key as its range id."""


class TruncatedDatabaseResultException(Exception):
    """Raised if the XML schema truncated over a subprocess interaction"""


class DuplicatePartitionException(Exception):
    """Raise if a partition being created already exists."""


class UnexpectedPartitionException(Exception):
    """Raised when the partition map is unexpected."""


class TableInformationException(Exception):
    """Raised when the table's status doesn't include the information we need."""


class NoEmptyPartitionsAvailableException(Exception):
    """Raised if no empty partitions are available to safely modify."""


class DatabaseCommandException(Exception):
    """Raised if the database command failed."""


class NoExactTimeException(Exception):
    """Raised if there's no exact time available for this partition."""

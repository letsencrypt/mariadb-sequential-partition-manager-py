import abc
import argparse
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse


def retention_from_dict(r):
    for k, v in r.items():
        if k == "days":
            return timedelta(days=v)
        else:
            raise argparse.ArgumentTypeError(
                f"Unknown retention period definition: {k}={v}"
            )


class Table:
    def __init__(self, name):
        self.name = SqlInput(name)
        self.retention = None
        self.partition_period = None

    def set_retention(self, ret):
        self.retention = ret

    def set_partition_period(self, dur):
        self.partition_period = dur

    def __str__(self):
        return f"Table {self.name}"


class SqlInput(str):
    valid_form = re.compile(r"^[A-Z0-9_-]+$", re.IGNORECASE)

    def __new__(cls, *args, **kwargs):
        if len(args) != 1:
            raise argparse.ArgumentTypeError(f"{args} is not a single argument")
        if not SqlInput.valid_form.match(args[0]):
            raise argparse.ArgumentTypeError(f"{args[0]} is not a valid SQL identifier")
        return super().__new__(cls, args[0])

    def __repr__(self):
        return str(self)


def toSqlUrl(urlstring):
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
    @abc.abstractmethod
    def run(self, sql):
        """
        Run the sql, returning the results or raising an Exception
        """


class Partition(abc.ABC):
    """
    Represents a single SQL table partition.
    """

    @abc.abstractmethod
    def values(self):
        """
        Return a SQL partition value string.
        """

    @property
    @abc.abstractmethod
    def name(self):
        """
        Return the partition's name.
        """

    @property
    @abc.abstractmethod
    def num_columns(self):
        """
        Return the number of columns this partition represents
        """

    def timestamp(self):
        """
        Returns a datetime object representing this partition's
        date, if the partition is of the form "p_YYYYMMDD", otherwise
        returns None
        """
        try:
            return datetime.strptime(self.name, "p_%Y%m%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return None

    def __repr__(self):
        return f"{type(self).__name__}<{str(self)}>"

    def __str__(self):
        return f"{self.name}: {self.values()}"


class PositionPartition(Partition):
    """
    A partition that may have positions assocated with it.
    """

    def __init__(self, name):
        self._name = name
        self.positions = list()

    @property
    def name(self):
        return self._name

    def add_position(self, position):
        self.positions.append(int(position))

    @property
    def num_columns(self):
        return len(self.positions)

    def values(self):
        return "(" + ", ".join([str(x) for x in self.positions]) + ")"

    def __lt__(self, other):
        if isinstance(other, MaxValuePartition):
            if len(self.positions) != other.num_columns:
                raise UnexpectedPartitionException(
                    f"Expected {len(self.positions)} columns but "
                    f"partition has {other.num_columns}."
                )
            return True
        other_positions = None
        if isinstance(other, list):
            other_positions = other
        elif isinstance(other, PositionPartition):
            other_positions = other.positions
        if not other_positions or len(self.positions) != len(other_positions):
            raise UnexpectedPartitionException(
                f"Expected {len(self.positions)} columns but partition has {other_positions}."
            )
        for v_mine, v_other in zip(self.positions, other_positions):
            if v_mine >= v_other:
                return False
        return True

    def __eq__(self, other):
        if isinstance(other, PositionPartition):
            return self._name == other._name and self.positions == other.positions
        return False


class MaxValuePartition(Partition):
    """
    A partition that lives at the tail of a partition list, saying
    all remaining values belong in this partition.
    """

    def __init__(self, name, count):
        self._name = name
        self.count = count

    @property
    def name(self):
        return self._name

    @property
    def num_columns(self):
        return self.count

    def values(self):
        return ", ".join(["MAXVALUE"] * self.count)

    def __lt__(self, other):
        """
        MaxValuePartitions are always greater than every other partition
        """
        if isinstance(other, list):
            if self.count != len(other):
                raise UnexpectedPartitionException(
                    f"Expected {self.count} columns but list has {len(other)}."
                )
            return False
        if isinstance(other, Partition):
            if self.count != other.num_columns:
                raise UnexpectedPartitionException(
                    f"Expected {self.count} columns but list has {other.num_columns}."
                )
            return False
        return ValueError()

    def __eq__(self, other):
        if isinstance(other, MaxValuePartition):
            return self._name == other._name and self.count == other.count
        return False


class MismatchedIdException(Exception):
    """
    Raised if the partition map doesn't use the primary key as its range id.
    """

    pass


class TruncatedDatabaseResultException(Exception):
    """
    Raised if the XML schema truncated over a subprocess interaction
    """

    pass


class DuplicatePartitionException(Exception):
    """
    Raise if a partition being created already exists.
    """

    pass


class UnexpectedPartitionException(Exception):
    """
    Raised when the partition map is unexpected.
    """

    pass


class TableInformationException(Exception):
    """
    Raised when the table's status doesn't include the information we need.
    """

    pass


class NoEmptyPartitionsAvailableException(Exception):
    """
    Raised if no empty partitions are available to safely modify.
    """

    pass

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

    def set_position(self, positions):
        self.positions = list(map(lambda p: int(p), positions))
        return self

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


class InstantPartition(PositionPartition):
    """
    Represent a partition at the current moment, used for rate calculations
    as a stand-in that only exists for the purposes of the rate calculation
    itself.
    """

    def __init__(self, now, positions):
        self._name = "Instant"
        self.instant = now
        self.positions = positions

    def timestamp(self):
        return self.instant


class ModifiedPartition(abc.ABC):
    def __init__(self):
        self.num_columns = None
        self.positions = None
        self._timestamp = None

    def set_timestamp(self, timestamp):
        """
        Set the timestamp to be used for the modified partition. This
        effectively changes the partition's name.
        """
        self._timestamp = timestamp
        return self

    def set_position(self, pos):
        """
        Set the position of this modified partition. If this partition
        changes an existing partition, the positions of both must have
        identical length.
        """
        if not isinstance(pos, list):
            raise ValueError()
        if self.num_columns is not None and len(pos) != self.num_columns:
            raise UnexpectedPartitionException(
                f"Expected {self.num_columns} columns but list has {len(pos)}."
            )
        self.positions = pos
        return self

    def timestamp(self):
        return self._timestamp

    @abc.abstractmethod
    def as_partition(self):
        """
        Return a Partition object representing this modified Partition
        """

    def __repr__(self):
        return f"{type(self).__name__}<{str(self)}>"

    def __eq__(self, other):
        if isinstance(other, ModifiedPartition):
            return (
                type(self) == type(other)
                and self.positions == other.positions
                and self._timestamp == other._timestamp
            )
        return False


class ChangedPartition(ModifiedPartition):
    """
    Represents modifications to a given Partition
    """

    def __init__(self, old_part):
        if not isinstance(old_part, Partition):
            raise ValueError()
        super().__init__()
        self.old = old_part
        self.num_columns = self.old.num_columns
        self._timestamp = self.old.timestamp()
        self.positions = (
            self.old.positions if isinstance(old_part, PositionPartition) else None
        )

    def as_partition(self):
        return PositionPartition(f"p_{self._timestamp:%Y%m%d}").set_position(
            self.positions
        )

    def __str__(self):
        return f"{self.old} => {self.positions} {self._timestamp}"


class NewPartition(ModifiedPartition):
    """
    Represents a wholly new Partition to be constructed
    """

    def __init__(self):
        super().__init__()

    def as_partition(self):
        if not self._timestamp:
            raise ValueError()
        if self.positions:
            return PositionPartition(f"p_{self._timestamp:%Y%m%d}").set_position(
                self.positions
            )
        raise ValueError("Positions not set, and not configured for MaxValue")

    def __str__(self):
        return f"Add: {self.positions} {self._timestamp}"


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

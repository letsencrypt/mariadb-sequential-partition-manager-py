import abc
import argparse
import re
from urllib.parse import urlparse


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


class MismatchedIdException(Exception):
    """
    Raised if the partition map doesn't use the primary key as its range id.
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

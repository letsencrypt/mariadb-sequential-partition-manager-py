import logging
import pymysql
import pymysql.cursors
import subprocess
import xml.parsers.expat

from collections import defaultdict
from partitionmanager.types import (
    DatabaseCommand,
    TruncatedDatabaseResultException,
    TableInformationException,
    SqlInput,
)


def destring(text):
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        pass
    return text


class XmlResult:
    """
    Ugly class to parse XML results from the mariadb CLI client. The general
    schema is:
    <resultset statement="sql query">
        <row>
            <field name="name" xsi:nil="true/false">data if any</field>
        </row>
    </resultset>

    The major hangups are that field can be nil, and field can also be
    of arbitrary size.
    """

    def __init__(self):
        self.logger = logging.getLogger(name="xml")

        self.xmlparser = xml.parsers.expat.ParserCreate()

        self.xmlparser.StartElementHandler = self.start_element
        self.xmlparser.EndElementHandler = self.end_element
        self.xmlparser.CharacterDataHandler = self.char_data

        self.rows = None
        self.current_row = None
        self.current_field = None
        self.current_elements = list()

    def parse(self, data):
        if self.rows is not None:
            raise ValueError("XmlResult objects can only be used once")

        self.rows = list()
        self.xmlparser.Parse(data)

        if len(self.current_elements) > 0:
            raise TruncatedDatabaseResultException(
                f"These XML tags are unclosed: {self.current_elements}"
            )
        return self.rows

    def start_element(self, name, attrs):
        self.logger.debug(
            f"Element start: {name} {attrs} (Current elements: {self.current_elements}"
        )
        self.current_elements.append(name)

        if name == "resultset":
            self.statement = attrs["statement"]
        elif name == "row":
            assert self.current_row is None
            self.current_row = defaultdict(str)
        elif name == "field":
            assert self.current_field is None
            self.current_field = attrs["name"]
            if "xsi:nil" in attrs and attrs["xsi:nil"] == "true":
                self.current_row[attrs["name"]] = None

    def end_element(self, name):
        self.logger.debug(
            f"Element end: {name} (Current elements: {self.current_elements}"
        )
        assert name == self.current_elements.pop()

        if name == "row":
            self.rows.append(self.current_row)
            self.current_row = None
        elif name == "field":
            assert self.current_field is not None
            value = self.current_row[self.current_field]
            if value:
                self.current_row[self.current_field] = destring(value)
            self.current_field = None

    def char_data(self, data):
        if self.current_elements[-1] == "field":
            assert self.current_field is not None
            assert self.current_row is not None

            self.current_row[self.current_field] += data


class SubprocessDatabaseCommand(DatabaseCommand):
    def __init__(self, exe):
        self.exe = exe

    def run(self, sql_cmd):
        result = subprocess.run(
            [self.exe, "-X"],
            input=sql_cmd,
            stdout=subprocess.PIPE,
            encoding="UTF-8",
            check=True,
        )
        return XmlResult().parse(result.stdout)

    def db_name(self):
        rows = self.run("SELECT DATABASE();")
        if len(rows) != 1:
            raise TableInformationException("Expected one result")

        return SqlInput(rows[0]["DATABASE()"])


class IntegratedDatabaseCommand(DatabaseCommand):
    def __init__(self, url):
        self.db = None
        if url.path and url.path != "/":
            self.db = url.path.lstrip("/")
        if not self.db:
            raise Exception("You must supply a database name")

        self.connection = pymysql.connect(
            host=url.hostname,
            port=url.port,
            user=url.username,
            password=url.password,
            database=self.db,
            cursorclass=pymysql.cursors.DictCursor,
        )

    def db_name(self):
        return SqlInput(self.db)

    def run(self, sql_cmd):
        with self.connection.cursor() as cursor:
            cursor.execute(sql_cmd)
            return [row for row in cursor]

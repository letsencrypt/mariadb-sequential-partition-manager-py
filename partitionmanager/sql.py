import subprocess
import pymysql
import pymysql.cursors
import xml.parsers.expat

from collections import defaultdict
from partitionmanager.types import DatabaseCommand, TableInformationException, SqlInput


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
    def __init__(self):
        self.xmlparser = xml.parsers.expat.ParserCreate()

        self.xmlparser.StartElementHandler = self.start_element
        self.xmlparser.EndElementHandler = self.end_element
        self.xmlparser.CharacterDataHandler = self.char_data

        self.rows = list()
        self.current_row = None
        self.current_field = None
        self.current_elements = list()

    def parse(self, data):
        self.xmlparser.Parse(data)
        return self.rows

    def start_element(self, name, attrs):
        print("Start element:", name, attrs)
        self.current_elements.append(name)
        if name == "resultset":
            self.statement = attrs["statement"]
        elif name == "row":
            assert self.current_row is None
            self.current_row = defaultdict(str)
        elif name == "field":
            assert self.current_field is None
            if "xsi:nil" in attrs and attrs["xsi:nil"] == "true":
                self.current_row[attrs["name"]] = None
            else:
                self.current_field = attrs["name"]

    def end_element(self, name):
        print("End element:", name)
        assert name == self.current_elements.pop()

        if name == "row":
            self.rows.append(self.current_row)
            self.current_row = None
        elif name == "field":
            self.current_row[self.current_field] = destring(
                self.current_row[self.current_field]
            )
            self.current_field = None

    def char_data(self, data):
        print(f"Character data for {self.current_elements[-1]}:", repr(data))

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

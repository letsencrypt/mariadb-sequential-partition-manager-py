import subprocess
import pymysql
import pymysql.cursors

from partitionmanager.types import DatabaseCommand


class SubprocessDatabaseCommand(DatabaseCommand):
    def __init__(self, exe):
        self.exe = exe

    def run(self, sql_cmd):
        result = subprocess.run(
            [self.exe, "-E"],
            input=sql_cmd,
            stdout=subprocess.PIPE,
            encoding="UTF-8",
            check=True,
        )
        return result.stdout


class IntegratedDatabaseCommand(DatabaseCommand):
    def __init__(self, url):
        self.connection = pymysql.connect(
            host=url.hostname,
            port=url.port,
            user=url.username,
            password=url.password,
            cursorclass=pymysql.cursors.DictCursor,
        )

    def run(self, sql_cmd):
        with self.connection.cursor() as cursor:
            cursor.execute(sql_cmd)
            for result in cursor.fetchone():
                yield result
        return

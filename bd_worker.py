import sqlite3
import pandas as pd


class DB:
    def __init__(self, patch: str):
        """
        Initialization of connection with the db

        param patch: Full db path
        """
        self.conn = sqlite3.connect(patch)
        self.cur = self.conn.cursor()

    def request(self, request: str, data: tuple):
        """
        Database query function

        :param data: Request parameter values
        :param request: Request body
        """
        self.cur.execute(request, data)
        return self.cur.fetchall()

    def statement(self, statement: str) -> None:
        """
        Non-parameterized database request

        :param statement: SQL statement
        """
        self.cur.execute(statement)
        self.conn.commit()

    def readTable(self, tableName: str) -> pd.DataFrame:
        """
        Reading table from db

        param tableName: Name of the table for proofreading
        """
        return pd.read_sql("select * from " + tableName, self.conn)

    def writeTable(self, statement: str, data: tuple) -> None:
        """
        Writing data to db

        :param statement:
        :param data: Tuple with row data
        """
        self.conn.execute(statement, data)
        self.conn.commit()

    def close(self):
        """
        Closing the connection with the database
        """
        self.conn.close()

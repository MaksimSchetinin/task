import tools
import pandas as pd
from bd_worker import DB
from datetime import datetime
from memory_profiler import profile


@profile
def taskOne(dataBase: DB) -> None:
    """
    Task 1 - Generating a table

    param dataBase: DB object with an open connection
    """
    dataBase.statement('''CREATE TABLE IF NOT EXISTS suspects (
        timestamp INTEGER,
        player_id INTEGER,
        event_id TEXT,
        error_id INTEGER,
        json_server TEXT,
        json_client TEXT)
        ''')


@profile
def task2_1(fileOnePatch: str, fileTwoPatch: str, startDate: int, stopDate: int) -> pd.DataFrame:
    """
    Subtask 2.1 - proofreading and joint

    param fileOnePatch: Path to the client file
    param fileTwoPatch: Path to the server file
    param startDate: Start date timestamp for filter
    param stopDate: Stop date timestamp for filter
    """
    clientDf = tools.fileRead(fileOnePatch,
                              {'timestamp': int, 'error_id': object, 'player_id': int, 'json_client': object},
                              startDate, stopDate)
    clientDf.drop('timestamp', axis=1, inplace=True)  # Getting rid of the wrong column
    serverDf = tools.fileRead(fileTwoPatch,
                              {'timestamp': int, 'event_id': int, 'error_id': object, 'json_server': object},
                              startDate, stopDate)

    result = pd.merge(clientDf, serverDf, on="error_id")  # merged dataFrames
    del clientDf
    del serverDf
    return result


@profile
def task2_2(dataBase: DB, data: pd.DataFrame):
    """
    Subtask 2.2 - filtering by database

    param data: Data after the first iteration
    param dataBase: DB object with an open connection
    """
    for index, row in data.iterrows():
        res = dataBase.request("""SELECT CASE
                                WHEN cheaters.ban_time <= DATETIME((?), '-1 day') THEN 'True'
                                ELSE 'False'
                                END AS result
                                FROM cheaters
                                WHERE player_id = (?);""", (tools.datetimeStr(row["timestamp"]), row["player_id"]))
        if tools.checkBanRes(res):  # Checking for cheating
            taskThree(dataBase, row)  # Run three task



def taskThree(dataBase: DB, data) -> None:
    """
    Task 3 - Database entry

    param dataBase: DB object with an open connection
    data dataBase: Row data
    """
    data['json_server'] = tools.jsonParser(data['json_server'])  # Pulling the target
    data['json_client'] = tools.jsonParser(data['json_client'])
    dataBase.writeTable("INSERT INTO suspects VALUES(?,?,?,?,?,?)",
                        (data['timestamp'], data['player_id'], data['event_id'],
                         data['error_id'], data['json_server'], data['json_client']))



def taskTwo(dataBase: DB, fileOnePatch: str, fileTwoPatch: str, startDate: datetime, stopDate: datetime):
    """
    Task 2 - Data processing

    param dataBase: DB object with an open connection
    param fileOnePatch: Path to the client file
    param fileTwoPatch: Path to the server file
    param startDate: Start date datetime for filter
    param stopDate: Stop date datetime for filter
    """
    data1 = task2_1(fileOnePatch, fileTwoPatch, tools.timestamp(startDate), tools.timestamp(stopDate))  # run subtask 1
    task2_2(dataBase, data1)  # run subtask 2


def main():
    db = DB('task/cheaters.db')  # Creating a connection to the database through the object
    clientFile = 'task/client.csv'
    serverFile = 'task/server.csv'
    taskOne(db)  # Completing the first task
    startDate = datetime(2021, 3, 14, 3, 20, 5)  # Setting the time frame
    stopDate = datetime(2021, 3, 14, 5, 20, 5)
    taskTwo(db, clientFile, serverFile, startDate, stopDate)  # Run second task
    db.close()  # Ending the session with the db


if __name__ == '__main__':
    main()

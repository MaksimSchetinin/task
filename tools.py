import json
import pandas as pd
from datetime import datetime
from memory_profiler import profile


# tools - a set of functions
# for data processing and proofreading

def timestamp(date: datetime) -> int:
    """
    Translation function
    from datetime to timestamp

    param date: Datetime
    """
    return int(round(date.timestamp()))


def datetimeStr(date: int) -> datetime:
    """
    Translation function
    from timestamp to datetime str

    param date: Timestamp
    """
    return datetime.fromtimestamp(date)

@profile
def fileRead(patch: str, dtypes: dict, date1: int, date2: int) -> pd.DataFrame:
    """

    param patch: Path to the .csv file
    param dtypes patch: Typing and headers for DataFrame
    param date1 patch: Start date timestamp for filter
    param date2 patch: Stop date timestamp for filter
    """
    df = pd.read_csv(patch, header=0, skiprows=1,
                     dtype=dtypes)
    df = df.set_axis(dtypes.keys(), axis=1)
    return df.query('@date1<timestamp<@date2')


def checkBanRes(res: list[tuple]) -> bool:
    """
    The function of analyzing the response
    from the database to search for experienced cheaters

    param res: Response from db
    """
    if res == [] or res[0][0] == 'False':
        return True
    return False


def jsonParser(jsonString: str) -> str:
    """
    Function for parsing 'purpose'

    param jsonString: Not a pretty line
    """
    return json.loads(jsonString).get('purpose')

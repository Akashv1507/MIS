import datetime as dt
from typing import TypedDict, List, Tuple


class FreqData(TypedDict):
    columns: List[str]
    rows: List[Tuple]


def getFreqFromDb(startDate: dt.datetime, endDate: dt.datetime) -> FreqData:
    """fetched the freq data for required dates

    Args:
        startDate (dt.datetime): start datetime object
        endDate (dt.datetime): end datetime object

    Returns:
        FreqData: The fetched freq data
    """
    return {'columns': [], 'rows': []}

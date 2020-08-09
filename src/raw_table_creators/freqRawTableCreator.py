import datetime as dt
from typing import List, Tuple
from src.fetchers.freqDbFetcherServer import getFreqFromDb
from src.repos.rawFrequencyRepo import rawFrequencyTodbRepo
def freqRawTableCreator(startDate:dt.datetime, endDate:dt.datetime,configDict) -> bool:
    """fetches raw frequency from reporting software and pushes into local db.

    Args:
        startDate (dt.datetime): start date
        endDate (dt.datetime): end date
        configDict ([type]): application configuration dictionary

    Returns:
        bool: true if insertion is successfull else false
    """    

    con_string= configDict['con_string_local']

    listOfTuples=getFreqFromDb(startDate,endDate,configDict)
    print(listOfTuples[:5])
    print(len(listOfTuples))

    Obj_rawFrequencyTodbRepo=rawFrequencyTodbRepo(con_string) # object of rawFrequencyTodbRepo class

    isInsertionSuccess=Obj_rawFrequencyTodbRepo.freqToDb(listOfTuples)

    return isInsertionSuccess
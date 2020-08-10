import datetime as dt
from typing import List, Tuple
from src.repos.derivedFreqRepo.freqFromDbToRecords import freqFromDbToRecords
from src.repos.derivedFreqRepo.derFreqParamInRecordsToDb import derFreqParamInRecordsToDb
def freqDerivedTableInsertion(startDate:dt.datetime, endDate:dt.datetime,configDict) -> bool:
    """fetches raw freq from raw_frequency table and pushes into derived_frequency

    Args:
        startDate (dt.datetime): start date
        endDate (dt.datetime): end date
        configDict ([type]): application configuration dictionary

    Returns:
        bool: true if insertion is successfull else false
    """    
    
    con_string= configDict['con_string_local']
    obj_freqFromDbToRecords = freqFromDbToRecords(con_string)
    obj_derFreqParamInRecordsToDb = derFreqParamInRecordsToDb(con_string)

    listOfTuples = obj_freqFromDbToRecords.fetchRawFreqFromDb(startDate,endDate)

    isInsertionSuccess = obj_derFreqParamInRecordsToDb.freqDerivedRecordsToDb(listOfTuples)

    return isInsertionSuccess
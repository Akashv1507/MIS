import datetime as dt
from typing import List, Tuple
from src.repos.derivedFreqRepo.freqFromDbToRecords import FreqFromDbToRecords
from src.repos.derivedFreqRepo.derFreqParamInRecordsToDb import DerFreqParamInRecordsToDb


def freqDerivedTableInsertion(startDate: dt.datetime, endDate: dt.datetime, configDict: dict) -> bool:
    """fetches raw freq from raw_frequency table and pushes into derived_frequency table in mis_warehouse db

    Args:
        startDate (dt.datetime): start date
        endDate (dt.datetime): end date
        configDict ([type]): application configuration dictionary

    Returns:
        bool: true if insertion is successfull else false
    """

    con_string = configDict['con_string_mis_warehouse']
    obj_freqFromDbToRecords = FreqFromDbToRecords(con_string)
    obj_derFreqParamInRecordsToDb = DerFreqParamInRecordsToDb(con_string)

    listOfTuples = obj_freqFromDbToRecords.fetchRawFreqFromDb(
        startDate, endDate)

    isInsertionSuccess = obj_derFreqParamInRecordsToDb.insertionOfFreqDerivedRecordsToDb(
        listOfTuples)

    return isInsertionSuccess

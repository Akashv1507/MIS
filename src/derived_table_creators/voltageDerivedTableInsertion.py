import datetime as dt
from typing import List, Tuple
from src.repos.derivedVoltageRepo.voltageFromDbtoRecords import VoltageFromDbToRecords
from src.repos.derivedVoltageRepo.derVoltageParamInRecordsToDb import DerVoltageParamInRecordsToDb

def voltageDerivedTableInsertion(startDate:dt.datetime, endDate:dt.datetime,configDict) -> bool:
    """fetch raw voltage from raw_voltage table->generate derived voltage fields->push derived voltage data into derived_voltage table in mis_warehouse db

    Args:
        startDate (dt.datetime): start-date
        endDate (dt.datetime): end-date
        configDict ([type]): application configuration dictionary

    Returns:
        bool: return True if insertion is successful else false.
    """    
    con_string= configDict['con_string_mis_warehouse']

    obj_voltageFromDbToRecords = VoltageFromDbToRecords(con_string)
    obj_derVoltageParamInRecordsToDb = DerVoltageParamInRecordsToDb(con_string)

    listOfTuples= obj_voltageFromDbToRecords.fetchRawVoltFromDb(startDate,endDate)
    isInsertionSuccess=obj_derVoltageParamInRecordsToDb.insertionOfVoltDerivedRecordsToDb(listOfTuples)
    return isInsertionSuccess

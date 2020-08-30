import datetime as dt
from typing import List, Tuple
from src.repos.derivedVDIRepo.fetchRawVoltage import FetchRawVoltage
from src.repos.derivedVDIRepo.insertionOfVDI import InsertionOfVDI


def VDIDerivedTableInsertion(startDate: dt.datetime, endDate: dt.datetime, configDict: dict) -> bool:
    """fetch raw voltage from raw_voltage table->generate derived VDI fields->push derived VDI data into derived_VDI table mis_warehouse db

    Args:
        startDate (dt.datetime): start-date
        endDate: end-date
        configDict ([type]): application configuration dictionary

    Returns:
        bool: return True if insertion is successful else false.
    """
    if endDate < startDate:
        print("start date should be less than end date")
        return False

    con_string = configDict['con_string_mis_warehouse']
    obj_fetchRawVoltage = FetchRawVoltage(con_string)
    obj_insertionOfVDI = InsertionOfVDI(con_string)

    # go back from start date till we get Monday
    reqStartDt = startDate
    while not reqStartDt.strftime("%A") == 'Monday':
        reqStartDt = reqStartDt - dt.timedelta(days=1)

    # go forward from end date till we get Sunday
    reqEndDt = endDate
    while not reqEndDt.strftime("%A") == 'Sunday':
        reqEndDt = reqEndDt + dt.timedelta(days=1)

    batchStartDt = reqStartDt
    batchEndDt = batchStartDt + dt.timedelta(days=6)

    while batchEndDt <= reqEndDt:
        listOfTuples = obj_fetchRawVoltage.fetchRawVoltFromDb(
            batchStartDt, batchEndDt)
        isInsertionSuccess = obj_insertionOfVDI.insertionOfVDI(listOfTuples)
        batchStartDt = batchStartDt + dt.timedelta(days=7)
        batchEndDt = batchEndDt + dt.timedelta(days=7)
    return isInsertionSuccess

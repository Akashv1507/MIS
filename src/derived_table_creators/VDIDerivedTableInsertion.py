import datetime as dt
from typing import List, Tuple
from src.repos.derivedVDIRepo.fetchRawVoltage import FetchRawVoltage
from src.repos.derivedVDIRepo.insertionOfVDI import InsertionOfVDI


def VDIDerivedTableInsertion(startDate:dt.datetime, endDate:dt.datetime,configDict : dict) -> bool:
    """fetch raw voltage from raw_voltage table->generate derived VDI fields->push derived VDI data into derived_VDI table mis_warehouse db

    Args:
        startDate (dt.datetime): start-date
        endDate: end-date
        configDict ([type]): application configuration dictionary

    Returns:
        bool: return True if insertion is successful else false.
    """    
    if startDate.strftime("%A") == 'Monday' and endDate.strftime("%A") == 'Sunday':
        con_string = configDict['con_string_mis_warehouse']

        obj_fetchRawVoltage = FetchRawVoltage(con_string)
        obj_insertionOfVDI = InsertionOfVDI(con_string)

        
        listOfTuples = obj_fetchRawVoltage.fetchRawVoltFromDb(startDate,endDate)
        # print(listOfTuples)
        isInsertionSuccess = obj_insertionOfVDI.insertionOfVDI(listOfTuples)
        
        return isInsertionSuccess
    else:
        print("invalid start-date/end-date , start-date should be monday and end-date should be sunday")
        return False

    
        
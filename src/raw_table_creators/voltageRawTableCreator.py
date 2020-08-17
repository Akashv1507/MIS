from src.repos.rawVoltageRepo import RawVoltageRepo
from src.fetchers.VoltageFetchFromExcel import voltageFetchFromExcel
import datetime as dt

def voltageRawTableCreator(date:dt.datetime ,configDict:dict) -> bool:
    """push raw voltage data into raw_voltage table in mis_warehouse database

    Args:
        configDict (dict): application dictionary

    Returns:
        bool: return true if insertion is successfull else false
    """    

    con_string = configDict['con_string_mis_warehouse']
    obj_rawVoltageRepo = RawVoltageRepo(con_string)

    dateStr = str(date)
    fileName = '\\VOLTTEMP_'+ dateStr[8:10] +'_' +dateStr[5:7] + '_' + dateStr[0:4] +'.csv'
    file_path = configDict['file_path'] + fileName

    listOfTuple = voltageFetchFromExcel(file_path)

    isInsertionSuccess = obj_rawVoltageRepo.insertionRawVoltToDb(listOfTuple)

    return isInsertionSuccess




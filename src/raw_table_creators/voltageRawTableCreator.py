from src.repos.rawVoltageRepo import rawVoltageRepo

def voltageRawTableCreator(configDict:dict) -> bool:
    """push raw voltage data into local database

    Args:
        configDict (dict): application dictionary

    Returns:
        bool: return true if insertion is successfull else false
    """    

    con_string = configDict['con_string_local']
    file_path = configDict['file_path'] + '\\VOLTTEMP_29_07_2019.csv'

    obj_rawVoltageRepo = rawVoltageRepo(con_string,file_path)

    isInsertionSuccess = obj_rawVoltageRepo.voltToDb()

    return isInsertionSuccess




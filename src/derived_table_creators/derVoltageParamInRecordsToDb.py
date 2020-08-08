import cx_Oracle
import datetime as dt
from typing import List, Tuple
def voltDerivedRecordsToDb(data:List[Tuple],configDict:dict) -> bool:
    """push derived voltage data into local database.

    Args:
        data (List[Tuple]): (DATE_KEY,MAPPING_ID,NODE_SCADA_NAME,NODE_NAME,MINIMUM,MAXIMUM,AVERAGE)
        configDict (dict): app configuration dictionary

    Returns:
        bool: returns true if insertion is successful else false
    """    
    delData=[]
    # creating List of tuple (dateKey,nodeName),unique constraint, based on which deletion take place before insertion. 
    for row in data: 
        dateKey=row[0]
        nodeName=row[3]
        delTuple=(dateKey,nodeName)  
        delData.append(delTuple)

    try:
        connString=configDict['con_string_local']
        connection=cx_Oracle.connect(connString)
        isInsertionSuccess= True

    except Exception as err:
        print('error while creating a connection',err)
    else:
        print(connection.version)
        try:
            cur=connection.cursor()
            try:
                cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD' ")
                del_sql="DELETE FROM derived_voltage where DATE_KEY= :1 AND NODE_NAME= :2 "
                insert_sql="INSERT INTO derived_voltage(DATE_KEY,MAPPING_ID,NODE_SCADA_NAME,NODE_NAME,MINIMUM,MAXIMUM,AVERAGE) VALUES(:1, :2, :3, :4, :5, :6, :7)"
                cur.executemany(del_sql,delData)
                cur.executemany(insert_sql, data)
                
            except Exception as e :
                print("error during deletion/insertion-", e)
                isInsertionSuccess=False

        except Exception as err:
            print('error while creating a cursor',err)
            isInsertionSuccess=False
        else:
            connection.commit()
    finally:
        cur.close()
        connection.close()
    return isInsertionSuccess
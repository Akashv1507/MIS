import cx_Oracle
import pandas as pd
import datetime as dt
from typing import List, Tuple
def freqToDb(listOfTuples: List[Tuple],configDict: dict) -> bool:
    """push list of tuples in the form (time_stamp,frequency) into local db

    Args:
        listOfTuples (List[Tuple]):  data in the form of list of tuples that is to be pushed into database
        configDict (dict): app configuration

    Returns:
        bool: true if insertion is successsfull
    """    
    
    try:
        con_string= configDict['con_string_local']
        connection= cx_Oracle.connect(con_string)
        isInsertionSuccess = True

    except Exception as err:
        print('error while creating a connection',err)
    else:
        print(connection.version)
        try:
            cur=connection.cursor()
            insert_sql="INSERT INTO FREQUENCY2(time_stamp,frequency) VALUES(:timestamp, :frequency)"
            cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS' ")
            cur.executemany(insert_sql,listOfTuples)

        except Exception as err:
            print('error while creating a cursor',err)
            isInsertionSuccess = False

        else:
            print('Insertion complete')
            connection.commit()
    finally:
        cur.close()
        connection.close()
    return isInsertionSuccess


import cx_Oracle
import pandas as pd
import datetime as dt
from typing import List, Tuple
def toRecordss(df:pd.core.frame.DataFrame) -> List[Tuple]:
    """convert data frame into list of tuple

    Args:
        df (pd.core.frame.DataFrame): pandas dataframe

    Returns:
        List[Tuple]: list of tuple in the form (date_key, mapping_id, Node_SCADA_name, node_name, minimum, maximum, average)
    """    
    data=[]
    for ind in df.index:
        tuple_value=(str(df['DATE_KEY'][ind])[:10], int(df['MAPPING_ID'][ind]),df['NODE_SCADA_NAME'][ind], df['NODE_NAME'][ind], float(df['MIN'][ind]), float(df['MAX'][ind]), float(df['AVG'][ind]) )
        data.append(tuple_value)
    return data


def fetchRawVoltFromDb(startDateKey: dt.datetime, endDateKey: dt.datetime,configDict: dict) -> List[Tuple]:
    """fetches raw voltage data from local db and returns list of tuple in the form
       (date_key, mapping_id, Node_SCADA_name,node_name, minimum, maximum, average)

    Args:
        startDateKey (dt.datetime): start date
        endDateKey (dt.datetime): end date
        configDict (dict): app configuration dictionary.

    Returns:
        List[Tuple]: return list of tuple of derived voltage parameters
    """    
    
    start_time_value=str(startDateKey.date())
    end_time_value=str(endDateKey.date())
    start_time_value= start_time_value + " 00:00:00"
    end_time_value= end_time_value + " 23:59:00"
    try:
        connString=configDict['con_string_local']
        connection=cx_Oracle.connect(connString)

    except Exception as err:
        print('error while creating a connection',err)
    else:
        print(connection.version)
        try:
            cur=connection.cursor()
            fetch_sql='''select trunc(vt.time_stamp) date_key,max(mt.ID) Mapping_ID ,vt.node_scada_name,min(mt.node_name)Node_name,min(vt.voltage_value) min,max(vt.voltage_value) max,avg(vt.voltage_value) avg
from mapping_table mt,voltage vt 
where mt.NODE_SCADA_NAME = vt.NODE_SCADA_NAME and vt.time_stamp between to_date(:start_time) and to_date(:end_time)
group by vt.node_scada_name,trunc(vt.time_stamp)
order by date_key,mapping_id'''
            cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS' ")
            df=pd.read_sql(fetch_sql,params={'start_time' : start_time_value,'end_time': end_time_value},con=connection)
                 
           
            
        except Exception as err:
            print('error while creating a cursor',err)
        else:
            print('retrieval complete')
            connection.commit()
    finally:
        cur.close()
        connection.close()
        print("connection closed")
    data=toRecordss(df) 
    return data

        
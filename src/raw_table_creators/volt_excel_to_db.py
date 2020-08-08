import pandas as pd
import cx_Oracle
from typing import List, Tuple


def filterVoltage(df: pd.core.frame.DataFrame)-> pd.core.frame.DataFrame:
    """return dataframe that contain filtered voltage.

    Args:
        df (pd.core.frame.DataFrame): dataframe without filtering.

    Returns:
        pd.core.frame.DataFrame: dataframe with filtering.
    """    
    for col in df.columns.tolist()[1:]:
        prev=df[col][0]
        for ind in df.index.tolist()[1:]:
            if col[-6]== '4':
                if 375 <= df[col][ind] <= 445:
                    pass
                else :
                    df[col][ind]=prev
            else:
                if 725 <= df[col][ind] <= 815:
                    pass
                else :
                    df[col][ind]=prev
            prev=df[col][ind]

    return df

def dfToListOfTuples(df: pd.core.frame.DataFrame)-> List[Tuple]:
    """convert dataframe that contain per minute volatge value of each node to list of tuple(time_stamp,node_scada_name,voltage_value)

    Args:
        df (pd.core.frame.DataFrame): dataframe that contain per minute volatge value.

    Returns:
        List[Tuple]: data=[(time_stamp,node_scada_name,voltage_value)]
    """    
    data=[]
    for ind in df.index:
        for col in df.columns.tolist()[1:]:
            tuple_value=(df['Timestamp'][ind],col,int(df[col][ind]))
            data.append(tuple_value)
    return data

def voltToDb(configDict: dict) -> bool:
    """read per minute voltage value of each node from excel file(VOLTTEMP_dd_mm_yyyy.csv) and push into local database

    Args:
        configDict (dict): app configuration

    Returns:
        bool: returns true if insertion is successfull.
    """    
    
    path=configDict['file_path'] + '\\VOLTTEMP_28_07_2019.csv'
    df=pd.read_csv(path,skiprows=2,skipfooter=7)
    df=filterVoltage(df)
    data=dfToListOfTuples(df) #data contains list of tuples
    try:
        con_string= configDict['con_string_local']
        connection= cx_Oracle.connect(con_string)
        isInsertSuccess=True
    except Exception as err:
        print('error while creating a connection',err)
    else:
        print(connection.version)
        try:
            cur=connection.cursor()
            insert_sql="INSERT INTO voltage(time_stamp,node_scada_name,voltage_value) VALUES(:timestamp, :station_name, :voltage_value)"
            cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'DD-MM-YYYY HH24:MI:SS' ")
            cur.executemany(insert_sql,data)

        except Exception as err:
            print('error while creating a cursor',err)
            isInsertSuccess= False

        else:
            print('Insertion complete')
            connection.commit()
    finally:
        cur.close()
        connection.close()
    return isInsertSuccess


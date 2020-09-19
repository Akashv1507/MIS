import pandas as pd
import cx_Oracle
from typing import List, Tuple
import datetime as dt


def filterVoltage(voltageDf: pd.core.frame.DataFrame, mappingDf: pd.core.frame.DataFrame) -> pd.core.frame.DataFrame:
    """return dataframe that contain filtered voltage.

    Args:
        voltageDf (pd.core.frame.DataFrame): dataframe of voltage values without filtering.
        mappingDf : dataframe of node_scada_name and node_voltage

    Returns:
        pd.core.frame.DataFrame: dataframe with filtering.
    """
    
    nodeList = voltageDf.columns.tolist()
    # nodeList = ['DBPWR 4B1 KV']
    # filtering logic
    for index in mappingDf.index:
        nodeName = mappingDf['NODE_SCADA_NAME'][index]
        nodeVoltage = mappingDf['NODE_VOLTAGE'][index]
        if nodeName in nodeList: 
            if nodeVoltage == 400:
                prev = voltageDf[nodeName][0]
                if voltageDf[nodeName][0] < 375 or voltageDf[nodeName][0] > 445:
                    voltageDf[nodeName][0] = 400
                    prev = voltageDf[nodeName][0]
                    # print(nodeName)
                for ind in voltageDf.index.tolist()[1:]:
                    if voltageDf[nodeName][ind]< 375 or voltageDf[nodeName][ind] > 445:
                        voltageDf[nodeName][ind] = prev
                    prev = voltageDf[nodeName][ind]
            elif nodeVoltage == 765:
                prev = voltageDf[nodeName][0]
                if voltageDf[nodeName][0] < 725 or voltageDf[nodeName][0] > 815:
                    voltageDf[nodeName][0] = 765
                    prev = voltageDf[nodeName][0]
                    # print(nodeName)
                for ind in voltageDf.index.tolist()[1:]:
                    if 725 <= voltageDf[nodeName][ind] <= 815:
                        pass
                    else:
                        voltageDf[nodeName][ind] = prev
                    prev = voltageDf[nodeName][ind]
    return voltageDf


def dfToListOfTuples(df: pd.core.frame.DataFrame) -> List[Tuple]:
    """convert dataframe that contain per minute volatge value of each node to list of tuple(time_stamp,node_scada_name,voltage_value)

    Args:
        df (pd.core.frame.DataFrame): dataframe that contain per minute volatge value.

    Returns:
        List[Tuple]: data=[(time_stamp,node_scada_name,voltage_value)]
    """
    data: List[Tuple] = []
    for ind in df.index:
        for col in df.columns.tolist()[1:]:
            tuple_value = (df['Timestamp'][ind], col, float(df[col][ind]))
            data.append(tuple_value)
    return data


def voltageFetchFromExcel(file_path: str, con_string:str) -> List[Tuple]:
    """fetch raw voltage from excel file of type (VOLTTEMP_28_07_2019.csv ) and return list of tuples

    Args:
        file_path (str): [description]

    Returns:
        List[Tuple]: (time_stamp,node_scada_name,voltage_value)
    """

    voltageDf = pd.read_csv(file_path, skiprows=2, skipfooter=7)
    try:
            # connString=configDict['con_string_local']
            connection = cx_Oracle.connect(con_string)

    except Exception as err:
        print('error while creating a connection', err)
    else:
        print(connection.version)
        try:
            cur = connection.cursor()
            fetch_sql = "SELECT node_voltage,node_scada_name from voltage_mapping_table"
            # cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS' ")
            mappingDf = pd.read_sql(fetch_sql, con=connection)
            # print(mappingDf.head())
        except Exception as err:
            print('error while creating a cursor', err)
        else:
            connection.commit()
    finally:
        cur.close()
        connection.close()
        print("connection closed")
    filterdVoltageDf = filterVoltage(voltageDf ,mappingDf)
    # print(filterdVoltageDf[['Timestamp','DBPWR 4B1 KV']][840:900])
    data = dfToListOfTuples(filterdVoltageDf)  # data contains list of tuples
    return data

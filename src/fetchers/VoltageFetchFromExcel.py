import pandas as pd
import cx_Oracle
from typing import List, Tuple
import datetime as dt
def filterVoltage(df: pd.core.frame.DataFrame)-> pd.core.frame.DataFrame:
        """return dataframe that contain filtered voltage.

        Args:
            df (pd.core.frame.DataFrame): dataframe without filtering.

        Returns:
            pd.core.frame.DataFrame: dataframe with filtering.
        """    
        for col in df.columns.tolist()[1:]:
            prev=df[col][0]
            if col[-6]== '4' or col[-7]== '4' or col[-2:] =='RY':
                for ind in df.index.tolist()[1:]:
                    if 375 <= df[col][ind] <= 445:
                        pass
                    else :
                        df[col][ind]=prev
                prev=df[col][ind]
            elif col[-6]== '7' :
                for ind in df.index.tolist()[1:]:

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

def voltageFetchFromExcel(file_path :str) -> List[Tuple]:
    """fetch raw voltage from excel file of type (VOLTTEMP_28_07_2019.csv ) and return list of tuples

    Args:
        file_path (str): [description]

    Returns:
        List[Tuple]: (time_stamp,node_scada_name,voltage_value)
    """    
    
    df = pd.read_csv(file_path, skiprows=2, skipfooter=7)
    df = filterVoltage(df)
    data = dfToListOfTuples(df) #data contains list of tuples
    return data



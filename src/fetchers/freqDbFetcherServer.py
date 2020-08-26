import cx_Oracle
import pandas as pd
import datetime as dt
from typing import List, Tuple


def toRequiredFormat(df: pd.core.frame.DataFrame) -> List[Tuple]:
    """convert df from  (Date_key,Time_key,Freq_val)/(20180222,9:57:30,49.9) -->>(Timestamp,freq_val)/(20180222 9:57:30, 49.9)
        and return list of tuple

    Args:
        df (pd.core.frame.DataFrame): pandas dataframe

    Returns:
        List[Tuple]: return list of tuple in the form (Timestamp,freq_val)/(2018-02-22 9:57:30, 49.9)
    """
    # resetting column name
    df.rename(columns={'DATE_KEY': 'date_key',
                       'TIME_KEY': 'time_key', 'FREQ_VAL': 'freq_val'}, inplace=True)
    # converting date_key and time_key column to string
    df['date_key'] = df['date_key'].astype(str)
    df['time_key'] = df['time_key'].astype(str)

    # adding "-" ex 20170216->2107-02-16
    for ind in df.index:
        df['date_key'][ind] = df['date_key'][ind][:4] + "-" + \
            df['date_key'][ind][4:6] + "-" + df['date_key'][ind][6:]

    # concatenating two string columns
    df['date_key'] = df[['date_key', 'time_key']].apply(
        lambda x: ' '.join(x), axis=1)
    # deleting time_key column that we got from database & dropping index
    del df['time_key']
    df.rename(columns={'date_key': 'Timestamp'}, inplace=True)

    # converting dataframe to list of tuple so that executemany() insertion takes place
    df.reset_index(drop=True, inplace=True)
    # converting to list of records
    records = df.to_records(index=False)
    data = list(records)
    return data


def getFreqFromDb(startDate: dt.datetime, endDate: dt.datetime, configDict: dict) -> List[Tuple]:
    """fetch raw frequency(per 10 sec value) from 10.2.100.56 (Date_key,Time_key,Freq_val) -> (20180222,9:57:30,49.9) &
       return list of tuples

    Args:
        startDate (dt.datetime): start date
        endDate (dt.datetime): end date
        configDict (dict): app configuration dictionary

    Returns:
        List[Tuple]: list of tuple in the form (Timestamp,freq_val)-> ('2019-07-24 06:12:00', 49.8861)
    """
    startDateValue = str(startDate.date())
    endDateValue = str(endDate.date())
    startDateValue = startDateValue[:4] + \
        startDateValue[5:7]+startDateValue[8:10]
    endDateValue = endDateValue[:4]+endDateValue[5:7]+endDateValue[8:10]
    try:
        connString = configDict['con_string_server_db']
        connection = cx_Oracle.connect(connString)

    except Exception as err:
        print('error while creating a connection', err)
    else:
        print(connection.version)
        try:
            cur = connection.cursor()
            fetch_sql = "SELECT DATE_KEY, TIME_KEY, FREQ_VAL FROM reporting_uat.stg_scada_frequency_nldc WHERE date_key>= :start_date AND date_key<= :end_date "
            # cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS' ")
            df = pd.read_sql(fetch_sql, params={
                             'start_date': startDateValue, 'end_date': endDateValue}, con=connection)
        except Exception as err:
            print('error while creating a cursor', err)
        else:
            print('retrieval of raw frequency from reporting software complete')
            connection.commit()
    finally:
        cur.close()
        connection.close()
    listOfTuple = toRequiredFormat(df)
    return listOfTuple

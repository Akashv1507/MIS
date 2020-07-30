
"""
Return dataframe that has columns datetime & freq_val
startDate - datetime object
endDate - datetime object
returns - dataframe that has columns datetime (datetime object) & freq_val(number)
"""
def toRequiredFormat(df):
    import pandas as pd
    #resetting column name 
    df.rename(columns={'DATE_KEY':'date_key', 'TIME_KEY':'time_key','FREQ_VAL':'freq_val'},inplace=True)
    #converting date_key and time_key column to string
    df['date_key']=df['date_key'].astype(str)
    df['time_key']=df['time_key'].astype(str)

    #adding "-" ex 20170216->2107-02-16
    for ind in df.index:
        df['date_key'][ind] =df['date_key'][ind][:4] + "-" + df['date_key'][ind][4:6] + "-" + df['date_key'][ind][6:]

    #concatenating two string columns
    df['date_key'] = df[['date_key', 'time_key']].apply(lambda x: ' '.join(x), axis = 1) 
    # deleting time_key column that we got from database & dropping index
    del df['time_key']
    # converting dataframe to list of tuple so that executemany() insertion takes place
    df.reset_index(drop=True, inplace=True)
    #converting to list of records
    records=df.to_records(index=False)
    data=list(records)
    return data

def getFreqFromDb(startDate,endDate,configDict):
    import cx_Oracle
    import pandas as pd
    try:
        connString=configDict['con_string_server_db2']
        connection=cx_Oracle.connect(connString)

    except Exception as err:
        print('error while creating a connection',err)
    else:
        print(connection.version)
        try:
            cur=connection.cursor()
            fetch_sql="SELECT DATE_KEY, TIME_KEY, FREQ_VAL FROM stg_scada_frequency_nldc WHERE date_key>= :start_date AND date_key<= :end_date "
            # cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS' ")
            df=pd.read_sql(fetch_sql,params={'start_date' : startDate,'end_date': endDate},con=connection)
            # print(df)
            listOfTuple=toRequiredFormat(df)
            
        except Exception as err:
            print('error while creating a cursor',err)
        else:
            print('retrieval complete')
            connection.commit()
    finally:
        cur.close()
        connection.close()
        return listOfTuple
        
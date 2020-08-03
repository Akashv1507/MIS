def readFreqExcel(configDict):
    import pandas as pd 
    import cx_Oracle
    path=configDict['file_path'] + '\\frequency.xlsx'
    df=pd.read_excel(path,names=['timestamp','frequency'])
    df['timestamp']=df['timestamp'].astype(str)
    # print(type(df['timestamp'][0]))
    records=df.to_records(index=False)
    records=tuple(map(tuple, records))
    data=list(records)
    # print(type(data[0]))
    # print(data)
    # print(str(configDict['con_string_local']))
    try:
        con_string= configDict['con_string_local']
        connection= cx_Oracle.connect(con_string)

    except Exception as err:
        print('error while creating a connection',err)
    else:
        print(connection.version)
        try:
            cur=connection.cursor()
            insert_sql="INSERT INTO FREQUENCY2(time_stamp,frequency) VALUES(:timestamp, :frequency)"
            cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS' ")
            cur.executemany(insert_sql,data)

        except Exception as err:
            print('error while creating a cursor',err)

        else:
            print('Insertion complete')
            connection.commit()
    finally:
        cur.close()
        connection.close()




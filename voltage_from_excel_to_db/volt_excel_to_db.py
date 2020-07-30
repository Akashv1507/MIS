def filterVoltage(df):
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

def dfToListOfTuples(df):
    # columns_list=df.columns.tolist
    data=[]
    for ind in df.index:
        for col in df.columns.tolist()[1:]:
            tuple_value=(df['Timestamp'][ind],col,int(df[col][ind]))
            data.append(tuple_value)
    return data

def voltToDb(configDict):
    import pandas as pd 
    import cx_Oracle
    path=configDict['file_path'] + '\\VOLTTEMP_22_07_2019.csv'
    df=pd.read_csv(path,skiprows=2,skipfooter=7)
    df=filterVoltage(df)
    data=dfToListOfTuples(df) #data contains list of tuples
    print(type(data[0][0]))
    try:
        con_string= configDict['con_string_local']
        connection= cx_Oracle.connect(con_string)
    except Exception as err:
        print('error while creating a connection',err)
    else:
        print(connection.version)
        try:
            cur=connection.cursor()
            insert_sql="INSERT INTO voltage(time_stamp,station_name,voltage_value) VALUES(:timestamp, :station_name, :voltage_value)"
            cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'DD-MM-YYYY HH24:MI:SS' ")
            cur.executemany(insert_sql,data)

        except Exception as err:
            print('error while creating a cursor',err)

        else:
            print('Insertion complete')
            connection.commit()
    finally:
        cur.close()
        connection.close()


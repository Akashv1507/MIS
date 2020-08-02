def freqDerivedRecordsToDb(data,configDict):
    import cx_Oracle
    try:
        connString=configDict['con_string_local']
        connection=cx_Oracle.connect(connString)

    except Exception as err:
        print('error while creating a connection',err)
    else:
        print(connection.version)
        try:
            cur=connection.cursor()
            for rows in data:
                try:
                    insert_sql="INSERT INTO derivedFreq(DATE_KEY,MAXIMUM,MINIMUM,AVERAGE,LESS_THAN_BAND,BETWEEN_BAND,GREATER_THAN_BAND,OUT_OF_BAND,OUT_OF_BAND_INHRS,FDI) VALUES(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)"
                    cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD' ")
                    cur.execute(insert_sql, rows)
                    print("unique record")
                except Exception as e :
                    print("duplicate records")

            # print(type(df['TIME_STAMP'][0]))
            
            
        except Exception as err:
            print('error while creating a cursor',err)
        else:
            # print('INSERTION complete')
            connection.commit()
    finally:
        cur.close()
        connection.close()
        print("connection closed")
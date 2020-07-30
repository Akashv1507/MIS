def freqToDb(listOfTuples,configDict):
    import cx_Oracle
    
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
            cur.executemany(insert_sql,listOfTuples)

        except Exception as err:
            print('error while creating a cursor',err)

        else:
            print('Insertion complete')
            connection.commit()
    finally:
        cur.close()
        connection.close()


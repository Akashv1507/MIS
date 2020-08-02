def voltDerivedRecordsToDb(data,configDict):
    countu=0
    countd=0
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
                    insert_sql="INSERT INTO derived_voltage(DATE_KEY,MAPPING_ID,NODE_SCADA_NAME,NODE_NAME,MINIMUM,MAXIMUM,AVERAGE) VALUES(:1, :2, :3, :4, :5, :6, :7)"
                    # print("yaha pahuncha")
                    cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD' ")
                    # print("yaha bhi pahuncha")
                    cur.execute(insert_sql, rows)
                    print("unique record")
                    countu=countu+1
                    print(countu)
                except Exception as e :
                    print(e)
                    print("duplicate records")
                    countd=countd+1
                    print(countd)

            # print(type(df['TIME_STAMP'][0]))
            
            
        except Exception as err:
            print('error while creating a cursor',err)
        else:
            print('INSERTION complete')
            connection.commit()
    finally:
        cur.close()
        connection.close()
        print("connection closed")
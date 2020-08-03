def toRecordss(df):
    data=[]
    for ind in df.index:
        tuple_value=(str(df['DATE_KEY'][ind])[:10], int(df['MAPPING_ID'][ind]),df['NODE_SCADA_NAME'][ind], df['NODE_NAME'][ind], float(df['MIN'][ind]), float(df['MAX'][ind]), float(df['AVG'][ind]) )
        data.append(tuple_value)
    return data


def fetchRawVoltFromDb(startDateKey,endDateKey,configDict):
    import cx_Oracle
    import pandas as pd
    startDateKey=str(startDateKey.date())
    endDateKey=str(endDateKey.date())
    start_time_value= startDateKey + " 00:00:00"
    end_time_value= endDateKey + " 23:59:00"
    try:
        connString=configDict['con_string_local']
        connection=cx_Oracle.connect(connString)

    except Exception as err:
        print('error while creating a connection',err)
    else:
        print(connection.version)
        try:
            cur=connection.cursor()
            fetch_sql='''select trunc(vt.time_stamp) date_key,max(mt.ID) Mapping_ID ,vt.node_scada_name,min(mt.node_name)Node_name,min(vt.voltage_value) min,max(vt.voltage_value) max,avg(vt.voltage_value) avg
from mapping_table mt,voltage vt 
where mt.NODE_SCADA_NAME = vt.NODE_SCADA_NAME and vt.time_stamp between to_date(:start_time) and to_date(:end_time)
group by vt.node_scada_name,trunc(vt.time_stamp)
order by date_key,mapping_id'''
            cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS' ")
            df=pd.read_sql(fetch_sql,params={'start_time' : start_time_value,'end_time': end_time_value},con=connection)
            data=toRecordss(df)      
           
            
        except Exception as err:
            print('error while creating a cursor',err)
        else:
            print('retrieval complete')
            connection.commit()
    finally:
        cur.close()
        connection.close()
        print("connection closed")
        return data

        
import cx_Oracle
import pandas as pd


def voltageMappingTable(configDict: dict):
    ''' read voltage_mapping_data from defined path and push voltage mapping data into mis_warehouse db'''

    file_path = configDict['voltage_files_folder_path'] + '\\voltage_mapping_data.csv'
    df = pd.read_csv(file_path)
    data = []
    for ind in df.index:
        tuple_value = (int(df['Id'][ind]), df['SCADA ID'][ind], int(df['Node Voltage'][ind]), df['Node Name'][ind], df['Node Full Name']
                       [ind], df['Node Scada Name'][ind], df['Is Included Daily Voltage'][ind],  df['Is Included Weekly VDI'][ind])
        data.append(tuple_value)
    conn = cx_Oracle.connect(configDict['con_string_mis_warehouse'])
    print(conn.version)
    cur = conn.cursor()
    # existingEntityRows = [(x[0],)for x in data]
    # cur.executemany("delete from voltage_mapping_table where id =: 1", existingEntityRows)
    insert_sql = "INSERT INTO voltage_mapping_table VALUES(:1, :2, :3, :4, :5, :6, :7, :8)"
    cur.executemany(insert_sql, data)
    conn.commit()
    cur.close()
    conn.close()
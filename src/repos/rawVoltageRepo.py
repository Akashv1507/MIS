import pandas as pd
import cx_Oracle
from typing import List, Tuple


class RawVoltageRepo():
    """
     repositroy to push raw voltage data into local db
    """

    def __init__(self, con_string: str) -> None:
        """constructor method

        Args:
            con_string (str): connection string

        """
        self.con_string = con_string

    def insertionRawVoltToDb(self, data: List[Tuple]) -> bool:
        """ push list of tuples (time_stamp,node_scada_name,voltage_value) into raw_voltage table in local database

        Args:
            data - List of tuples

        Returns:
            bool: returns true if insertion is successfull.
        """

        # path=configDict['file_path'] + '\\VOLTTEMP_28_07_2019.csv'

        try:
            # con_string= configDict['con_string_local']
            connection = cx_Oracle.connect(self.con_string)
            isInsertSuccess = True
        except Exception as err:
            print('error while creating a connection', err)
        else:
            print(connection.version)
            try:
                cur = connection.cursor()
                insert_sql = "INSERT INTO raw_voltage(time_stamp,node_scada_name,voltage_value) VALUES(:timestamp, :station_name, :voltage_value)"
                cur.execute(
                    "ALTER SESSION SET NLS_DATE_FORMAT = 'DD-MM-YYYY HH24:MI:SS' ")
                cur.executemany(insert_sql, data)

            except Exception as err:
                print('error while creating a cursor', err)
                isInsertSuccess = False

            else:
                print('Insertion of raw_voltage complete')
                connection.commit()
        finally:
            cur.close()
            connection.close()
        return isInsertSuccess

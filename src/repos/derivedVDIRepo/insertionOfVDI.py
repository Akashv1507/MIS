import cx_Oracle
import datetime as dt
from typing import List, Tuple


class InsertionOfVDI():
    """repository to push derived VDI parameters in derived_VDI table in mis_warehouse db.
    """

    def __init__(self, con_string: str) -> None:
        """initialize connection string

        Args:
            con_string ([type]): connection string 
        """
        self.connString = con_string

    def insertionOfVDI(self, data: List[Tuple]) -> bool:
        """Insert data to mis_warehouse derived_VDI table

        Args:
            self : object of class InsertionOfVDI()
            data (List[Tuple]): (mapping_id, week_start_date, node_name, node_voltage, maximum, minimum, less_than_band, between_band, greater_than_band, less_than_band_inHrs, greater_than_band_inHrs, out_of_band_inHrs, VDI) 

        Returns:
            bool: return true if insertion is successful else false
        """
        delData = []
        for row in data:
            weekStartDate = row[1]
            nodeName = row[2]
            tempTuple = (weekStartDate, nodeName)
            # making list of tuple of (weekstartDate,nodeName)(unique), based on which deletion takes place before insertion of duplicate
            delData.append(tempTuple)

        try:
            # connString=configDict['con_string_local']
            connection = cx_Oracle.connect(self.connString)
            isInsertionSuccess = True

        except Exception as err:
            print('error while creating a connection', err)
        else:
            print(connection.version)
            try:
                cur = connection.cursor()
                try:
                    cur.execute(
                        "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD' ")
                    del_sql = "DELETE FROM derived_VDI WHERE week_start_date = :1 AND node_name = :2"
                    cur.executemany(del_sql, delData)
                    insert_sql = "INSERT INTO derived_VDI(mapping_id, week_start_date, node_name, node_voltage, maximum, minimum, less_than_band, between_band, greater_than_band, less_than_band_inHrs, greater_than_band_inHrs, out_of_band_inHrs, VDI) VALUES(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13)"
                    cur.executemany(insert_sql, data)
                except Exception as e:
                    print("error while insertion/deletion->", e)
                    isInsertionSuccess = False
            except Exception as err:
                print('error while creating a cursor', err)
                isInsertionSuccess = False
            else:
                connection.commit()
        finally:
            cur.close()
            connection.close()
        return isInsertionSuccess

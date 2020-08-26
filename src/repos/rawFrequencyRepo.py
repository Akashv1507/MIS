import cx_Oracle
import pandas as pd
import datetime as dt
from typing import List, Tuple


class RawFrequencyTodbRepo():
    """raw frequency(push) repo
    """

    def __init__(self, con_string) -> None:
        """initialize connection string, constructor method

        Args:
            con_string ([type]): connection string 
        """
        self.con_string = con_string

    def insertionOfFrequencyToDb(self, listOfTuples: List[Tuple]) -> bool:
        """push list of tuples in the form (time_stamp,frequency) into raw_frequency table in local db

        Args:
            listOfTuples (List[Tuple]):  data in the form of list of tuples that is to be pushed into database

        Returns:
            bool: true if insertion is successsfull
        """

        try:
            # con_string= configDict['con_string_local']
            connection = cx_Oracle.connect(self.con_string)
            isInsertionSuccess = True

        except Exception as err:
            print('error while creating a connection', err)
        else:
            print(connection.version)
            try:
                cur = connection.cursor()
                insert_sql = "INSERT INTO raw_frequency(time_stamp,frequency) VALUES(:timestamp, :frequency)"
                cur.execute(
                    "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS' ")
                cur.executemany(insert_sql, listOfTuples)

            except Exception as err:
                print('error while creating a cursor', err)
                isInsertionSuccess = False

            else:
                print('Insertion of raw frequency complete')
                connection.commit()
        finally:
            cur.close()
            connection.close()
        return isInsertionSuccess

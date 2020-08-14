import cx_Oracle
import datetime as dt
from typing import List, Tuple
class DerVoltageParamInRecordsToDb():
    """ repository class to push derived voltage records to derived_voltage table in local db
    """    
    def __init__(self,con_string:str) ->None:
        """ constructor method

        Args:
            con_string (str): [description]
        """        
        self.connString=con_string

    def insertionOfVoltDerivedRecordsToDb(self,data:List[Tuple]) -> bool:
        """push derived voltage data into local database.

        Args:
            self: obeject of derVoltageParamInRecordsToDb()
            data (List[Tuple]): (DATE_KEY,MAPPING_ID,NODE_SCADA_NAME,NODE_NAME,MINIMUM,MAXIMUM,AVERAGE)

        Returns:
            bool: returns true if insertion is successful else false
        """    
        delData=[]
        # creating List of tuple (dateKey,nodeName),unique constraint, based on which deletion take place before insertion. 
        for row in data: 
            dateKey=row[0]
            nodeName=row[3]
            delTuple=(dateKey,nodeName)  
            delData.append(delTuple)

        try:
            # connString=configDict['con_string_local']
            connection=cx_Oracle.connect(self.connString)
            isInsertionSuccess= True

        except Exception as err:
            print('error while creating a connection',err)
        else:
            print(connection.version)
            try:
                cur=connection.cursor()
                try:
                    cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD' ")
                    del_sql="DELETE FROM derived_voltage where DATE_KEY= :1 AND NODE_NAME= :2 "
                    insert_sql="INSERT INTO derived_voltage(DATE_KEY,MAPPING_ID,NODE_SCADA_NAME,NODE_NAME,MINIMUM,MAXIMUM,AVERAGE) VALUES(:1, :2, :3, :4, :5, :6, :7)"
                    cur.executemany(del_sql,delData)
                    cur.executemany(insert_sql, data)
                    
                except Exception as e :
                    print("error during deletion/insertion-", e)
                    isInsertionSuccess=False

            except Exception as err:
                print('error while creating a cursor',err)
                isInsertionSuccess=False
            else:
                connection.commit()
        finally:
            cur.close()
            connection.close()
        return isInsertionSuccess
import cx_Oracle
import datetime as dt
from typing import List, Tuple

class derFreqParamInRecordsToDb():
    """repository to push derived frequency parameters in derived_frequency table in local db.
    """    
    def __init__(self,con_string:str) ->None:
        """initialize connection string

        Args:
            con_string ([type]): connection string 
        """        
        self.connString=con_string

    def freqDerivedRecordsToDb(self,data:List[Tuple]) -> bool:
        """Insert data to local derived frequency table

        Args:
            self : object of class derFreqParamInRecordsToDb()
            data (List[Tuple]): (DATE_KEY,MAXIMUM,MINIMUM,AVERAGE,LESS_THAN_BAND,BETWEEN_BAND,GREATER_THAN_BAND,OUT_OF_BAND,OUT_OF_BAND_INHRS,FDI)
            
        Returns:
            bool: return true if insertion is successful else false
        """    
        delData=[]
        for row in data:
            dateKey=(row[0],)
            delData.append(dateKey)       # making list of tuple of date_keys(unique), based on which deletion takes place before insertion of duplicate  
        
        try:
            # connString=configDict['con_string_local']
            connection=cx_Oracle.connect(self.connString)
            isInsertionSuccess = True

        except Exception as err:
            print('error while creating a connection',err)
        else:
            print(connection.version)
            try:
                cur=connection.cursor()
                try:
                    cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD' ")
                    del_sql="DELETE FROM derived_frequency WHERE DATE_KEY = :1 "
                    cur.executemany(del_sql, delData)
                    insert_sql="INSERT INTO derived_frequency(DATE_KEY,MAXIMUM,MINIMUM,AVERAGE,LESS_THAN_BAND,BETWEEN_BAND,GREATER_THAN_BAND,OUT_OF_BAND,OUT_OF_BAND_INHRS,FDI) VALUES(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)"
                    cur.executemany(insert_sql, data)
                except Exception as e :
                    print("error while insertion/deletion->" ,e) 
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
import cx_Oracle
import pandas as pd
import datetime as dt
from typing import List, Tuple


class FreqFromDbToRecords():
    """derived frequency(fetch) repository
    """

    def __init__(self, con_string):
        """initialize connection string

        Args:
            con_string ([type]): connection string 
        """
        self.connString = con_string

    def derivedFieldsCalculation(self, df: pd.core.frame.DataFrame) -> List[Tuple]:
        """convert dataframe into list of tuples each tuple having derived freq parameters-
            (date,maxValue,minValue,avgValue,lessThanIegcBand,betweenIegcBand,greaterThanIegcBand,outOfIegcBand,NoOfHrsFreqOutOfBand,FDI)

        Args:
            df (pd.core.frame.DataFrame): df as a dataframe
            self: object of class freqFromDbToRecords()

        Returns:
            List[Tuple]: list of tuples of derived freq parameters
        """
        def lessThan(lstOfFreq):
            count = 0
            for i in lstOfFreq:
                if i < 49.90:
                    count = count+1
            return (count/len(lstOfFreq))*100

        def greaterThan(lstOfFreq):
            count = 0
            for i in lstOfFreq:
                if i > 50.05:
                    count = count+1
            return (count/len(lstOfFreq))*100

        def average(lstOfFreq):
            sum = 0
            for i in lstOfFreq:
                sum = sum + i
            return (sum/len(lstOfFreq))

        df['Dates'] = pd.to_datetime(df['TIME_STAMP']).dt.date
        df['Time'] = pd.to_datetime(df['TIME_STAMP']).dt.time
        del df['TIME_STAMP']
        data: List[Tuple] = []
        groupedDates = df.groupby("Dates")
        for nameOfGroup, groupDf in groupedDates:
            # print(nameOfGroup)
            # print(groupDf.head())

            date = str(nameOfGroup)
            maxValue = groupDf['FREQUENCY'].max()
            minValue = groupDf['FREQUENCY'].min()
            avgValue = average(groupDf['FREQUENCY'].values)
            lessThanIegcBand = lessThan(
                groupDf['FREQUENCY'].values)  # percentage of time
            greaterThanIegcBand = greaterThan(
                groupDf['FREQUENCY'].values)  # percentage of time
            # percentage of time
            betweenIegcBand = 100-(lessThanIegcBand+greaterThanIegcBand)
            outOfIegcBand = lessThanIegcBand + greaterThanIegcBand  # percentage of time
            NoOfHrsFreqOutOfBand = (outOfIegcBand*24)/100  # In no. of hrs
            FDI = NoOfHrsFreqOutOfBand/24
            tempTuple = (date, maxValue, minValue, avgValue, lessThanIegcBand, betweenIegcBand,
                         greaterThanIegcBand, outOfIegcBand, NoOfHrsFreqOutOfBand, FDI)
            data.append(tempTuple)
        return data

    def fetchRawFreqFromDb(self, startDateKey: dt.datetime, endDateKey: dt.datetime) -> List[Tuple]:
        """returns derived freq fields in form of list of tuples ,each tuple belongs to a day
            (date,maxValue,minValue,avgValue,lessThanIegcBand,betweenIegcBand,greaterThanIegcBand,outOfIegcBand,NoOfHrsFreqOutOfBand,FDI)

        Args:
            self: object of class freqFromDbToRecords()
            startDateKey (dt.datetime): start-date
            endDateKey (dt.datetime): end-date


        Returns:
            List[Tuple]: list of tuples of derived freq parameters
        """

        startDate = str(startDateKey.date())
        endDate = str(endDateKey.date())
        start_time_value = startDate + " 00:00:00"
        end_time_value = endDate + " 23:59:50"
        try:
            # connString=configDict['con_string_local']
            connection = cx_Oracle.connect(self.connString)

        except Exception as err:
            print('error while creating a connection', err)
        else:
            print(connection.version)
            try:
                cur = connection.cursor()
                fetch_sql = "SELECT time_stamp, frequency FROM raw_frequency WHERE time_stamp BETWEEN TO_DATE(:start_time,'YYYY-MM-DD HH24:MI:SS') and TO_DATE(:end_time,'YYYY-MM-DD HH24:MI:SS') ORDER BY ID"
                # cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS' ")
                df = pd.read_sql(fetch_sql, params={
                                 'start_time': start_time_value, 'end_time': end_time_value}, con=connection)

            except Exception as err:
                print('error while creating a cursor', err)
            else:
                print('retrieval of raw frequency completed')
                connection.commit()
        finally:
            cur.close()
            connection.close()
            print("connection closed")
        listOfTuples = self.derivedFieldsCalculation(df)
        return listOfTuples

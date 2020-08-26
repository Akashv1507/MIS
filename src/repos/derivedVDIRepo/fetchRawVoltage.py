import cx_Oracle
import pandas as pd
import datetime as dt
from typing import List, Tuple
# import copy


class FetchRawVoltage():
    """Repository class for fetching raw voltage from local db and generating derived VDI voltage record
    """

    def __init__(self, con_string: str):
        """ constructor method 

        Args:
            con_string (str): connection-string
        """
        self.connString = con_string

    def toVDIFieldsCalculation(self, df: pd.core.frame.DataFrame) -> List[Tuple]:
        """convert data frame into list of tuple

        Args:
            df (pd.core.frame.DataFrame): pandas dataframe

        Returns:
            List[Tuple]: list of tuple in the form (mapping_id, week_start_date, node_name, node_voltage, maximum, minimum, less_than_band, between_band, greater_than_band, less_than_band_inHrs, greater_than_band_inHrs, out_of_band_inHrs, VDI)
        """
        def lessThan(nodeVoltage, lstOfVoltValues):
            count = 0
            if nodeVoltage == 765:
                for i in lstOfVoltValues:
                    if i < 728:
                        count = count + 1
            elif nodeVoltage == 400:
                for i in lstOfVoltValues:
                    if i < 380:
                        count = count + 1
            return (count/len(lstOfVoltValues))*100

        def greaterThan(nodeVoltage, lstOfVoltValues):
            count = 0
            if nodeVoltage == 765:
                for i in lstOfVoltValues:
                    if i > 800:
                        count = count + 1
            elif nodeVoltage == 400:
                for i in lstOfVoltValues:
                    if i > 420:
                        count = count + 1
            return (count/len(lstOfVoltValues))*100

        df['date'] = df['TIME_STAMP'].dt.date
        df['date'] = df['date'].apply(
            lambda x: x - dt.timedelta(days=x.weekday()))
        # del df['TIME_STAMP']

        data: List[Tuple] = []
        group = df.groupby(['NODE_NAME', 'date'])
        for nameOfGroup, groupDf in group:

            mappingId = int(groupDf['MAPPING_ID'].max())
            date = str(groupDf['date'].min())
            nodeName = nameOfGroup[0]
            nodeVoltage = int(groupDf['NODE_VOLTAGE'].min())
            weeklyMaximum = int(groupDf['VOLTAGE_VALUE'].max())
            weeklyMinimum = int(groupDf['VOLTAGE_VALUE'].min())
            lessThanIegcBand = lessThan(
                nodeVoltage, groupDf['VOLTAGE_VALUE'].values.tolist())
            greaterThanIegcBand = greaterThan(
                nodeVoltage, groupDf['VOLTAGE_VALUE'].values.tolist())
            betweenIegcBand = 100 - \
                (lessThanIegcBand + greaterThanIegcBand)  # percentage of time
            lessThanIegcBandInHrs = lessThanIegcBand * 1.68
            greaterThanIegcBandInHrs = greaterThanIegcBand * 1.68
            outOfBandInHrs = lessThanIegcBandInHrs + greaterThanIegcBandInHrs
            VDI = outOfBandInHrs/168
            tempTuple = (mappingId, date, nodeName, nodeVoltage, weeklyMaximum, weeklyMinimum, lessThanIegcBand,
                         betweenIegcBand, greaterThanIegcBand, lessThanIegcBandInHrs, greaterThanIegcBandInHrs, outOfBandInHrs, VDI)
            data.append(tempTuple)

        return data

    def fetchRawVoltFromDb(self, startDateKey: dt.datetime, endDateKey: dt.datetime) -> List[Tuple]:
        """fetches raw voltage data from mis_warehouse and returns list of tuple in the form
        (mapping_id, week_start_date, node_name, node_voltage, maximum, minimum, less_than_band, between_band, greater_than_band, less_than_band_inHrs, greater_than_band_inHrs, out_of_band_inHrs, VDI)

        Args:
            self : object of class FetchRawVoltage
            startDateKey (dt.datetime): start date
            endDateKey (dt.datetime): end date

        Returns:
            List[Tuple]: return list of tuple of derived VDI parameters
        """

        start_time_value = str(startDateKey.date())
        end_time_value = str(endDateKey.date())
        start_time_value = start_time_value + " 00:00:00"
        end_time_value = end_time_value + " 23:59:00"
        try:
            # connString=configDict['con_string_local']
            connection = cx_Oracle.connect(self.connString)

        except Exception as err:
            print('error while creating a connection', err)
        else:
            print(connection.version)
            try:
                cur = connection.cursor()
                fetch_sql = '''select (mt.ID)mapping_Id, vt.time_stamp Time_Stamp,mt.node_voltage, mt.node_name Node_Name,vt.voltage_value
                        from voltage_mapping_table mt, raw_voltage vt 
                        where mt.NODE_SCADA_NAME = vt.NODE_SCADA_NAME and vt.time_stamp between to_date(:start_time) and to_date(:end_time)
                        order by time_stamp'''
                # fetch_sql ="select * from raw_voltage"

                cur.execute(
                    "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS' ")
                # print('yaha pahunch gya')
                df = pd.read_sql(fetch_sql, params={
                                 'start_time': start_time_value, 'end_time': end_time_value}, con=connection)
                print(df.head())

            except Exception as err:
                print('error while creating a cursor', err)
            else:
                print('retrieval raw voltage complete')
                connection.commit()
        finally:
            cur.close()
            connection.close()
            print("connection closed")
        listOfTuple = self.toVDIFieldsCalculation(df)
        return listOfTuple

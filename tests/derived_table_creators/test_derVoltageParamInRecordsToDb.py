import unittest
from src.fetchers.voltageFromDbtoRecords import fetchRawVoltFromDb
from src.derived_table_creators.derVoltageParamInRecordsToDb import voltDerivedRecordsToDb
from datetime import datetime as dt
from src.appConfig import getAppConfigDict

class freqDbFetcherServer(unittest.TestCase):
    appConfig = None

    def setUp(self):
        self.appConfig = getAppConfigDict()

    def test_run(self) -> None:
        startDate=dt.strptime("2019-07-24", '%Y-%m-%d')
        endDate=dt.strptime("2019-07-25", '%Y-%m-%d')
        result=fetchRawVoltFromDb(startDate,endDate,self.appConfig)
        self.assertTrue(voltDerivedRecordsToDb(result,self.appConfig))

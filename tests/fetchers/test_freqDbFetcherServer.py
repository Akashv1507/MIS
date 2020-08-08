import unittest
from src.fetchers.freqDbFetcherServer import getFreqFromDb
from datetime import datetime as dt
from src.appConfig import getAppConfigDict

class freqDbFetcherServer(unittest.TestCase):
    appConfig = None

    def setUp(self):
        self.appConfig = getAppConfigDict()

    def test_run(self) -> None:
        startDate=dt.strptime("2019-07-24", '%Y-%m-%d')
        endDate=dt.strptime("2019-07-25", '%Y-%m-%d')
        day=(endDate-startDate).days
        result=getFreqFromDb(startDate,endDate,self.appConfig)
        self.assertTrue(len(result)==(day+1)*8640)
        self.assertTrue(len(result[0])==2)
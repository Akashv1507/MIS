import unittest
from src.fetchers.freqDbFetcherServer import getFreqFromDb
from datetime import datetime as dt
from src.appConfig import getAppConfigDict


class testFreqDbFetcherServer(unittest.TestCase):
    """test whether raw frequency fetching from reporting s/w is success or not.

    Args:
        unittest ([type]): [description]
    """
    appConfig = None

    def setUp(self):
        self.appConfig = getAppConfigDict()

    def test_run(self) -> None:
        """test whether raw frequency fetching from reporting s/w is success or not
        """
        startDate = dt.strptime("2019-07-28", '%Y-%m-%d')
        endDate = dt.strptime("2019-07-28", '%Y-%m-%d')
        days = (endDate-startDate).days
        result = getFreqFromDb(startDate, endDate, self.appConfig)
        self.assertTrue(len(result) == (days+1)*8640)
        self.assertTrue(len(result[0]) == 2)

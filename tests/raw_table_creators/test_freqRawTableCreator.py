import unittest
from src.raw_table_creators.freqRawTableCreator import freqRawTableCreator
from datetime import datetime as dt
from src.appConfig import getAppConfigDict


class testFreqRawTableCreator(unittest.TestCase):
    """test whether frequency raw table insertion is success or not.

    Args:
        unittest ([type]): [description]
    """    
    appConfig = None

    def setUp(self):
        self.appConfig = getAppConfigDict()

    def test_run(self) -> None:
        startDate=dt.strptime("2019-07-30", '%Y-%m-%d')
        endDate=dt.strptime("2019-07-30", '%Y-%m-%d')
        
        self.assertTrue(freqRawTableCreator(startDate,endDate,self.appConfig))
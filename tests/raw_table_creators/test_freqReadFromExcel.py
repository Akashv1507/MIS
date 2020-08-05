import unittest
from src.raw_table_creators.freq_read_from_excel import readFreqExcel
import datetime as dt
from src.appConfig import getAppConfigDict

class testFreqReadFromExcel(unittest.TestCase):
    appConfig = None

    def setUp(self):
        self.appConfig = getAppConfigDict()
        
    def test_run(self) -> None:

        self.assertTrue(readFreqExcel(
            self.appConfig))
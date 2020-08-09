import unittest
from src.raw_table_creators.voltageRawTableCreator import voltageRawTableCreator
from datetime import datetime as dt
from src.appConfig import getAppConfigDict


class tesVoltageRawTableCreator(unittest.TestCase):
    """test whether raw voltage insertion is success

    Args:
        unittest ([type]): [description]
    """    
    appConfig = None

    def setUp(self):
        self.appConfig = getAppConfigDict()

    def test_run(self) -> None:
        
        self.assertTrue(voltageRawTableCreator(self.appConfig))
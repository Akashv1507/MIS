import unittest
from src.raw_table_creators.voltageRawTableCreator import voltageRawTableCreator
from datetime import datetime as dt
from src.appConfig import getAppConfigDict


class tesVoltageRawTableCreator(unittest.TestCase):
    """test whether raw_voltage table insertion is success or not.

    Args:
        unittest ([type]): [description]
    """    
    appConfig = None

    def setUp(self):
        self.appConfig = getAppConfigDict()

    def test_run(self) -> None:
        """test whether raw_voltage table insertion is success or not.
        """        
        
        self.assertTrue(voltageRawTableCreator(self.appConfig))
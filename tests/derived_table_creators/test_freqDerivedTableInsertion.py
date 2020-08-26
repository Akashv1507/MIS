import unittest
from src.derived_table_creators.freqDerivedTableInsertion import freqDerivedTableInsertion
from datetime import datetime as dt
from src.appConfig import getAppConfigDict


class testFreqDerivedTableInsertion(unittest.TestCase):
    """test whether derived_frequency table insertion is success or not.

    Args:
        unittest ([type]): [description]
    """
    appConfig = None

    def setUp(self):
        self.appConfig = getAppConfigDict()

    def test_run(self) -> None:
        """test whether derived_frequency table insertion is success or not.
        """
        startDate = dt.strptime("2019-07-29", '%Y-%m-%d')
        endDate = dt.strptime("2019-07-31", '%Y-%m-%d')

        self.assertTrue(freqDerivedTableInsertion(
            startDate, endDate, self.appConfig))

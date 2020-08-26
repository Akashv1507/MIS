import unittest
from src.derived_table_creators.voltageDerivedTableInsertion import voltageDerivedTableInsertion
from datetime import datetime as dt
from src.appConfig import getAppConfigDict


class testVoltageDerivedTableInsertion(unittest.TestCase):
    """test whether derived_voltage table insertion is success or not.

    Args:
        unittest ([type]): [description]
    """
    appConfig = None

    def setUp(self):
        self.appConfig = getAppConfigDict()

    def test_run(self) -> None:
        """test whether derived_voltage table insertion is success or not.
        """
        startDate = dt.strptime("2019-07-29", '%Y-%m-%d')
        endDate = dt.strptime("2019-07-31", '%Y-%m-%d')

        self.assertTrue(voltageDerivedTableInsertion(
            startDate, endDate, self.appConfig))

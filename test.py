import unittest
from tests.fetchers import test_freqFromDbToRecords
from tests.raw_table_creators import test_freqReadFromExcel


# initialize the test suite
loader = unittest.TestLoader()
freqFromDbTestSuite = unittest.TestSuite()
readFreqFromExcelTestSuite=unittest.TestSuite()


# add tests to the test suite
freqFromDbTestSuite.addTests(loader.loadTestsFromModule(test_freqFromDbToRecords))
readFreqFromExcelTestSuite.addTests(loader.loadTestsFromModule(test_freqReadFromExcel))

# initialize a runner, pass it your suite and run it
runner = unittest.TextTestRunner(verbosity=3)
# result1 = runner.run(freqFromDbTestSuite)
result2=runner.run(readFreqFromExcelTestSuite)
import unittest
from tests.raw_table_creators import test_freqRawTableCreator
# from tests.raw_table_creators import test_freqReadFromExcel


# initialize the test suite
loader = unittest.TestLoader()

freqRawTableCreator = unittest.TestSuite()
# readFreqFromExcelTestSuite=unittest.TestSuite()


# add tests to the test suite
freqRawTableCreator.addTests(loader.loadTestsFromModule(test_freqRawTableCreator)
# readFreqFromExcelTestSuite.addTests(loader.loadTestsFromModule(test_freqReadFromExcel))

# initialize a runner, pass it your suite and run it
runner=unittest.TextTestRunner(verbosity=3)
# result1 = runner.run(freqFromDbTestSuite)
result=runner.run(freqRawTableCreator)
from datetime import datetime as dt
from src.appConfig import getAppConfigDict
# from src.raw_table_creators.freq_read_from_excel import readFreqExcel
from src.fetchers.freqDbFetcherServer import getFreqFromDb
from src.raw_table_creators.voltageRawTableCreator import voltageRawTableCreator
from src.raw_table_creators.freqRawTableCreator import freqRawTableCreator
from src.derived_table_creators.freqDerivedTableInsertion import freqDerivedTableInsertion
from src.derived_table_creators.voltageDerivedTableInsertion import voltageDerivedTableInsertion

configDict=getAppConfigDict()
startDate=dt.strptime("2019-07-30", '%Y-%m-%d')
endDate=dt.strptime("2019-07-31", '%Y-%m-%d')


#repo code start.

# print(freqRawTableCreator(startDate,endDate,configDict))
# print(voltageRawTableCreator(configDict))

print(freqDerivedTableInsertion(startDate,endDate,configDict))
# print(voltageDerivedTableInsertion(startDate,endDate,configDict))







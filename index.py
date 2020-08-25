from datetime import datetime as dt
from src.appConfig import getAppConfigDict
from src.raw_table_creators.voltageMappingTable import voltageMappingTable
from src.fetchers.freqDbFetcherServer import getFreqFromDb
from src.raw_table_creators.voltageRawTableCreator import voltageRawTableCreator
from src.raw_table_creators.freqRawTableCreator import freqRawTableCreator
from src.derived_table_creators.freqDerivedTableInsertion import freqDerivedTableInsertion
from src.derived_table_creators.voltageDerivedTableInsertion import voltageDerivedTableInsertion
from src.derived_table_creators.VDIDerivedTableInsertion import VDIDerivedTableInsertion


configDict=getAppConfigDict()
startDate=dt.strptime("2020-07-06", '%Y-%m-%d')
endDate=dt.strptime("2020-07-12", '%Y-%m-%d')

# 1-frequency

# print(freqRawTableCreator(startDate,endDate,configDict))
# print(freqDerivedTableInsertion(startDate,endDate,configDict))

# 2-voltage

# voltageMappingTable(configDict)
# print(voltageRawTableCreator(startDate,configDict))
# print(voltageDerivedTableInsertion(startDate,endDate,configDict))

# 3- VDI
# print(VDIDerivedTableInsertion(startDate,endDate,configDict))


from datetime import datetime as dt
from configuration.appConfig import getAppConfigDict
from src.raw_table_creators.freq_read_from_excel import readFreqExcel
from src.fetchers.freqDbFetcherServer import getFreqFromDb
from src.raw_table_creators.freq_in_records_to_db import freqToDb
from src.raw_table_creators.volt_excel_to_db import voltToDb
from src.fetchers.freqFromDbToRecords import fetchRawFreqFromDb
from src.derived_table_creators.derFreqParamInRecordsToDb import freqDerivedRecordsToDb
from src.fetchers.voltageFromDbtoRecords import fetchRawVoltFromDb
from src.derived_table_creators.derVoltageParamInRecordsToDb import voltDerivedRecordsToDb
configDict=getAppConfigDict()
startDate=dt.strptime("2019-07-24", '%Y-%m-%d')
endDate=dt.strptime("2019-07-24", '%Y-%m-%d')


# read_freq_excel(configDict)
# listOfTuples=getFreqFromDb(startDate,endDate,configDict)
# print(listOfTuples[:5])
# print(len(listOfTuples))
# freqToDb(listOfTuples,configDict)

# voltToDb(configDict)

# listOfTuplesOfDerivedFreq = fetchRawFreqFromDb(startDate,endDate,configDict)
# print(listOfTuplesOfDerivedFreq)
# freqDerivedRecordsToDb(listOfTuplesOfDerivedFreq,configDict)

# data=fetchRawVoltFromDb(startDate,endDate,configDict)
# print(data[:5])
# voltDerivedRecordsToDb(data,configDict)








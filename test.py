from configuration.appConfig import getAppConfigDict
from Input_freq_from_excel_to_db.freq_read_from_excel import readFreqExcel
from fetchers.freq_db_fetcher import getFreqFromDb
from freq_in_records_to_db.freq_in_records_to_db import freqToDb
from voltage_from_excel_to_db.volt_excel_to_db import voltToDb
from derive_file_code.freqFromDbToRecords import fetchRawFreqFromDb
from derive_file_code.derFreqParamInRecordsToDb import freqDerivedRecordsToDb
from derive_file_code.voltageFromDbtoRecords import fetchRawVoltFromDb
from derive_file_code.derVoltageParamInRecordsToDb import voltDerivedRecordsToDb
configDict=getAppConfigDict()
# read_freq_excel(configDict)
# listOfTuples=getFreqFromDb(20190725,20190728,configDict)
# freqToDb(listOfTuples,configDict)

# voltToDb(configDict)

# listOfTuplesOfDerivedFreq = fetchRawFreqFromDb("2019-07-22","2019-07-28",configDict)
# print(len(listOfTuplesOfDerivedFreq))
# freqDerivedRecordsToDb(listOfTuplesOfDerivedFreq,configDict)

data=fetchRawVoltFromDb("2019-07-23","2019-07-25",configDict)
# print(data[:5])
voltDerivedRecordsToDb(data,configDict)








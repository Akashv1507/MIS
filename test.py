from configuration.appConfig import getAppConfigDict
from Input_freq_from_excel_to_db.freq_read_from_excel import readFreqExcel
from fetchers.freq_db_fetcher import getFreqFromDb
from freq_in_records_to_db.freq_in_records_to_db import freqToDb
from voltage_from_excel_to_db.volt_excel_to_db import voltToDb
from derive_file_code.freqFromDbToListOfTuples import fetchRawFreqFromDb
from derive_file_code.derFreqParamInListOftupleToDb import freqDerivedRecordsToDb
configDict=getAppConfigDict()
# read_freq_excel(configDict)
# listOfTuples=getFreqFromDb(20190221,20190221,configDict)
# freqToDb(listOfTuples,configDict)

# voltToDb(configDict)

listOfTuplesOfDerivedFreq = fetchRawFreqFromDb("2019-07-22","2019-07-22",configDict)
print(listOfTuplesOfDerivedFreq)
freqDerivedRecordsToDb(listOfTuplesOfDerivedFreq,configDict)






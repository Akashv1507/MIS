from configuration.appConfig import getAppConfigDict
from Input_freq_from_excel_to_db.freq_read_from_excel import readFreqExcel
from fetchers.freq_db_fetcher import getFreqFromDb
from freq_in_records_to_db.freq_in_records_to_db import freqToDb
from voltage_from_excel_to_db.volt_excel_to_db import voltToDb

configDict=getAppConfigDict()
# read_freq_excel(configDict)
# listOfTuples=getFreqFromDb(20190221,20190221,configDict)
# freqToDb(listOfTuples,configDict)

voltToDb(configDict)






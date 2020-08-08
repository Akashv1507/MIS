'''
# TODO write good decription below
This script creates the data mart for frequency and voltages in weekly report
## Steps
* read freq data from reporting software outages database
* transform it to fit the local raw data table and push into it
* read voltage data from excel files located at folder (# TODO describe it more better)
* transform it to fit the local raw data table and push into it
'''
from datetime import datetime as dt
from src.appConfig import getAppConfigDict
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


read_freq_excel(configDict)
listOfTuples=getFreqFromDb(startDate,endDate,configDict)
print(listOfTuples[:5])
print(len(listOfTuples))
freqToDb(listOfTuples,configDict)

voltToDb(configDict)

listOfTuplesOfDerivedFreq = fetchRawFreqFromDb(startDate,endDate,configDict)
print(listOfTuplesOfDerivedFreq)
freqDerivedRecordsToDb(listOfTuplesOfDerivedFreq,configDict)

data=fetchRawVoltFromDb(startDate,endDate,configDict)
print(data[:5])
voltDerivedRecordsToDb(data,configDict)








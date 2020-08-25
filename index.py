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
parser = argparse.ArgumentParser()
parser.add_argument('--start_date', help="Enter Start date in yyyy-mm-dd format")
parser.add_argument('--end_date', help="Enter end date in yyyy-mm-dd format")

args = parser.parse_args()
startDate = dt.strptime(args.start_date, '%Y-%m-%d')
endDate = dt.strptime(args.end_date, '%Y-%m-%d')

# 1-frequency

# print(freqRawTableCreator(startDate,endDate,configDict))
# print(freqDerivedTableInsertion(startDate,endDate,configDict))

# 2-voltage

# voltageMappingTable(configDict)
# print(voltageRawTableCreator(startDate,configDict))
# print(voltageDerivedTableInsertion(startDate,endDate,configDict))

# 3- VDI
# print(VDIDerivedTableInsertion(startDate,endDate,configDict))


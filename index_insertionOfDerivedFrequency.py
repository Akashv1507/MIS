import argparse
from datetime import datetime as dt
from src.appConfig import getAppConfigDict
from src.derived_table_creators.freqDerivedTableInsertion import freqDerivedTableInsertion


configDict=getAppConfigDict()

parser = argparse.ArgumentParser()
parser.add_argument('--start_date', help="Enter Start date in yyyy-mm-dd format")
parser.add_argument('--end_date', help="Enter end date in yyyy-mm-dd format")

args = parser.parse_args()
startDate = dt.strptime(args.start_date, '%Y-%m-%d')
endDate = dt.strptime(args.end_date, '%Y-%m-%d')

print(freqDerivedTableInsertion(startDate,endDate,configDict))

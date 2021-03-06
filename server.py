'''
This is the web server that acts as a service that creates raw/derived data of voltage and frequency
'''
import datetime as dt
from src.appConfig import getAppConfigDict
from src.raw_table_creators.freqRawTableCreator import freqRawTableCreator
from src.derived_table_creators.freqDerivedTableInsertion import freqDerivedTableInsertion
from src.raw_table_creators.voltageRawTableCreator import voltageRawTableCreator
from src.derived_table_creators.voltageDerivedTableInsertion import voltageDerivedTableInsertion
from src.derived_table_creators.VDIDerivedTableInsertion import VDIDerivedTableInsertion
from src.fetchersForUi.derFrequencyFetchers import DerivedFrequencyFetch
from flask import Flask, request, jsonify

app = Flask(__name__)

# get application config
appConfig = getAppConfigDict()

# Set the secret key to some random bytes
app.secret_key = appConfig['flaskSecret']

# creating instance of classes of fetchers for UI
con_string = appConfig['con_string_mis_warehouse']


@app.route('/')
def hello():
    return "This is the web service that acts as a service that creates raw/derived data of voltage and frequency"


@app.route('/rawFrequency', methods=['POST'])
def create_raw_frequency():
    # get start and end dates from post request body
    reqData = request.get_json()
    try:
        startDate = dt.datetime.strptime(reqData['startDate'], '%Y-%m-%d')
        endDate = dt.datetime.strptime(reqData['endDate'], '%Y-%m-%d')
    except Exception as ex:
        return jsonify({'message': 'Unable to parse start and end dates of this request body'}), 400
    # create raw frequency raw data between start and end dates
    isRawDataCreationSuccess = freqRawTableCreator(
        startDate, endDate, appConfig)
    if isRawDataCreationSuccess:
        return jsonify({'message': 'raw frequency data creation successful!!!', 'startDate': startDate, 'endDate': endDate})
    else:
        return jsonify({'message': 'raw frequency data creation was not success'}), 500


@app.route('/derivedFrequency', methods=['POST'])
def create_derived_frequency():
    # get start and end dates from post request body
    reqData = request.get_json()
    try:
        startDate = dt.datetime.strptime(reqData['startDate'], '%Y-%m-%d')
        endDate = dt.datetime.strptime(reqData['endDate'], '%Y-%m-%d')
    except Exception as ex:
        return jsonify({'message': 'Unable to parse start and end dates of this request body'}), 400
    # create frequency derived data between start and end dates
    isRawDataCreationSuccess = freqDerivedTableInsertion(
        startDate, endDate, appConfig)
    if isRawDataCreationSuccess:
        return jsonify({'message': 'derived frequency data creation successful!!!', 'startDate': startDate, 'endDate': endDate})
    else:
        return jsonify({'message': 'derived frequency data creation was not success'}), 500


@app.route('/rawVoltage', methods=['POST'])
def create_raw_voltage():
    # get start and end dates from post request body
    reqData = request.get_json()
    try:
        startDate = dt.datetime.strptime(reqData['startDate'], '%Y-%m-%d')
        endDate = dt.datetime.strptime(reqData['endDate'], '%Y-%m-%d')
    except Exception as ex:
        return jsonify({'message': 'Unable to parse start and end dates of this request body'}), 400
    # create voltage raw data between start and end dates
    isRawDataCreationSuccess = voltageRawTableCreator(
        startDate, endDate, appConfig)
    if isRawDataCreationSuccess:
        return jsonify({'message': 'raw voltage data creation successful!!!', 'startDate': startDate, 'endDate': endDate})
    else:
        return jsonify({'message': 'raw voltage data creation was not success'}), 500


@app.route('/derivedVoltage', methods=['POST'])
def create_derived_voltage():
    # get start and end dates from post request body
    reqData = request.get_json()
    try:
        startDate = dt.datetime.strptime(reqData['startDate'], '%Y-%m-%d')
        endDate = dt.datetime.strptime(reqData['endDate'], '%Y-%m-%d')
    except Exception as ex:
        return jsonify({'message': 'Unable to parse start and end dates of this request body'}), 400
    # create voltage derived data between start and end dates
    isRawDataCreationSuccess = voltageDerivedTableInsertion(
        startDate, endDate, appConfig)
    if isRawDataCreationSuccess:
        return jsonify({'message': ' derived voltage data creation successful!!!', 'startDate': startDate, 'endDate': endDate})
    else:
        return jsonify({'message': ' derived voltage data creation was not success'}), 500


@app.route('/derivedVdi', methods=['POST'])
def create_derived_vdi():
    # get start and end dates from post request body
    reqData = request.get_json()
    try:
        startDate = dt.datetime.strptime(reqData['startDate'], '%Y-%m-%d')
        endDate = dt.datetime.strptime(reqData['endDate'], '%Y-%m-%d')
    except Exception as ex:
        return jsonify({'message': 'Unable to parse start and end dates of this request body'}), 400
    # create vdi derived data between start and end dates
    isRawDataCreationSuccess = VDIDerivedTableInsertion(
        startDate, endDate, appConfig)
    if isRawDataCreationSuccess:
        return jsonify({'message': 'VDI derived data creation successful!!!', 'startDate': startDate, 'endDate': endDate})
    else:
        return jsonify({'message': 'VDI deived data data creation was not success'}), 500

@app.route('/getDerivedFrequency', methods=['GET'])
def get_derived_frequency():
    try:
        startDateStr = request.args.get('startDate', None, type=str)
        endDateStr = request.args.get('endDate', None, type=str)
        startDate = dt.datetime.strptime(startDateStr, '%Y-%m-%d')
        endDate = dt.datetime.strptime(endDateStr, '%Y-%m-%d')
    except Exception as ex:
        return jsonify({'message': 'Unable to parse start and end dates of this request body'}), 400
    # fetch frequency derived data between start and end dates,returns 1- derivedFrequencyDict['rows'] = derFreqRows|| 2- derivedFrequencyDict['weeklyFDI'] = weeklyFDI
    obj_derivedFrequencyFetch = DerivedFrequencyFetch(con_string)
    derivedFreqDict = obj_derivedFrequencyFetch.fetchDerivedFrequency(startDate, endDate)
    if derivedFreqDict:
        return jsonify({'message': 'Success!', 'data': derivedFreqDict , 'startDate': startDate, 'endDate': endDate})
    else:
        return jsonify({'message': 'Frequency derived data fetch unsuccessfull'}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(appConfig['flaskPort']), debug=True)

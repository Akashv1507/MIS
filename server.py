'''
This is the web server that acts as a service that creates outages raw data
'''
import datetime as dt
from src.appConfig import getAppConfigDict
from src.raw_table_creators.freqRawTableCreator import freqRawTableCreator
from flask import Flask, request, jsonify

app = Flask(__name__)

# get application config
appConfig = getAppConfigDict()

# Set the secret key to some random bytes
# app.secret_key = appConfig['flaskSecret']


@app.route('/')
def hello():
    return "This is the web server that acts as a service that creates outages raw data"


@app.route('/rawFrequency', methods=['POST'])
def create_raw_outages():
    # get start and end dates from post request body
    reqData = request.get_json()
    try:
        startDate = dt.datetime.strptime(reqData['startDate'], '%Y-%m-%d')
        endDate = dt.datetime.strptime(reqData['endDate'], '%Y-%m-%d')
    except Exception as ex:
        return jsonify({'message': 'Unable to parse start and end dates of this request body'}), 400
    # create outages raw data between start and end dates
    isRawDataCreationSuccess = freqRawTableCreator( startDate, endDate, appConfig)
    if isRawDataCreationSuccess:
        return jsonify({'message': 'raw data creation successful!!!', 'startDate': startDate, 'endDate': endDate})
    else:
        return jsonify({'message': 'raw data creation was not success'}), 500


if __name__ == '__main__':
    app.run(host = 'localhost', port=int(appConfig['flaskPort']), debug=True)
    # app.run(debug=True)

import requests
import json
from pymongo import MongoClient
#pymongo - version 4.1.1

with open('/tmp/pycharm_project_612/config.json') as f:
    config = json.load(f)
    rapidApi_key = config['rapidApi_key']

#create a connection
client = MongoClient('mongodb://localhost:27017')

#create db if not exists
mongo_db = client['stocks_db']

#create 'static_data' collection
mongo_collection = mongo_db['static_data']
cur = mongo_collection.find()
results = list(cur)

tickers_col = mongo_db['tickers']
stocks_list = tickers_col.distinct("ticker")

# Checking the cursor is empty--
if len(results) == 0:
    # a list that will contain all records
    static_data = []

    for stock in stocks_list:
        url = f"https://yahoo-finance15.p.rapidapi.com/api/yahoo/qu/quote/{stock}/asset-profile"

        headers = {
            "X-RapidAPI-Key": rapidApi_key,
            "X-RapidAPI-Host": "yahoo-finance15.p.rapidapi.com"
        }

        response = requests.request("GET", url, headers=headers)
        data = json.loads(response.text)
        industry = data['assetProfile']['industry']
        sector = data['assetProfile']['sector']
        summery = data['assetProfile']['longBusinessSummary']
        static_data.append({'ticker': stock, 'industry': industry, 'sector': sector, 'summery': summery})

        # insert all documents into the collection at once
        mongo_collection.insert_many(static_data)

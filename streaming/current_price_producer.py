import requests, json, pytz
from datetime import datetime
from kafka import KafkaProducer
from pymongo import MongoClient
from time import sleep


with open('/tmp/pycharm_project_54/config.json') as f:
    config = json.load(f)
    rapidApi_key = config['rapidApi_key']

topics = ['stocks_prices_to_mongo', 'stocks_prices_to_s3']
brokers = ['cnt7-naya-cdh63:9092']

producer = KafkaProducer(
	bootstrap_servers=brokers,
	client_id='producer',
	acks=1,
	compression_type=None,
	retries=3,
	reconnect_backoff_ms=50,
	# here we pass over the dict to kafka as json!
	value_serializer=lambda v: json.dumps(dict_current_price).encode('utf-8'))

client = MongoClient('mongodb://localhost:27017')
mongo_db = client['stocks_db']
users_collection = mongo_db['users']
users_list = list(users_collection.find())
is_there_at_least_one_active = users_collection.find_one({'is_active': 1})

tickers_col = mongo_db["tickers"]
# Get ticker names
stocks_list = tickers_col.distinct("ticker")

realtime_data_col = mongo_db['realtime_data']

while True:
	for stock in stocks_list:
		url = f"https://yahoo-finance15.p.rapidapi.com/api/yahoo/mo/module/{stock}"

		querystring = {"module": "asset-profile,financial-data,earnings"}

		headers = {
			"X-RapidAPI-Key": rapidApi_key,
			"X-RapidAPI-Host": "yahoo-finance15.p.rapidapi.com"
		}

		response = requests.request("GET", url, headers=headers, params=querystring)
		data = json.loads(response.text)
		if 'financialData' in data :
			current_price = data['financialData']['currentPrice']['raw']

			# getting the last document in mongodb for this stock_ticker
			last_doc = list(realtime_data_col.find({"stock_ticker": stock}).sort("time",-1).limit(1))

			dict_current_price = {'stock_ticker': stock, 'current_price': current_price,
								  'time': str(datetime.now(pytz.timezone('US/Eastern')))}

			if len(last_doc) == 0:
				# if there is no doc yet
				if len(users_list) > 0 and is_there_at_least_one_active:
					producer.send(topic='stocks_prices_kafka', value=dict_current_price)
					producer.flush()

				for topic in topics:
					producer.send(topic=topic, value=dict_current_price)
					producer.flush()

			# continue to stream process only if the current price has changed since the last saved price
			else :
				# send to email process only if the current price has changed since the last saved one
				if round(float(last_doc[0]["current_price"]),2) != round(float(current_price),2):
					# send to email process only if there are users requests or active one
					if len(users_list) > 0 and is_there_at_least_one_active:
						producer.send(topic='stocks_prices_kafka', value=dict_current_price)
						producer.flush()
					for topic in topics:
						producer.send(topic=topic, value=dict_current_price)
						producer.flush()

	sleep(1)
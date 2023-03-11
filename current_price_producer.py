import requests, json, pytz
from datetime import datetime
from kafka import KafkaProducer
from utils import stocks_list
from pymongo import MongoClient
from time import sleep


with open('/tmp/pycharm_project_139/config.json') as f:
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
	#here we pass over the dict to kafka as json!
	value_serializer=lambda v: json.dumps(dict_current_price).encode('utf-8'))

client = MongoClient('mongodb://localhost:27017')
mongo_db = client['stocks_db']
mongo_collection = mongo_db['users']
users_list = list(mongo_collection.find())
is_there_at_least_one_active = mongo_collection.find_one({'is_active': 1})

# while True:
for stock in stocks_list:
	url = f"https://yahoo-finance15.p.rapidapi.com/api/yahoo/mo/module/{stock}"

	querystring = {"module": "asset-profile,financial-data,earnings"}

	headers = {
		"X-RapidAPI-Key": rapidApi_key,
		"X-RapidAPI-Host": "yahoo-finance15.p.rapidapi.com"
	}

	response = requests.request("GET", url, headers=headers, params=querystring)
	data = json.loads(response.text)
	dict_current_price = {'stock_ticker': stock, 'current_price': data['financialData']['currentPrice']['fmt'], 'time': str(datetime.now(pytz.timezone('US/Eastern')))}

	# send to email process only if there are users requests or active one
	if len(users_list) > 0 and is_there_at_least_one_active:
		producer.send(topic='stocks_prices_kafka', value=dict_current_price)
		producer.flush()

	for topic in topics:
		producer.send(topic=topic, value=dict_current_price)
		producer.flush()

sleep(1)
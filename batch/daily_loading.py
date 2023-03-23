import json
import mysql.connector
from pymongo import MongoClient
import batch.get_daily_data as data
from datetime import datetime, timedelta

dir = '/tmp/pycharm_project_488'

# Get configuration data
with open(f'{dir}/config.json') as f:
    config = json.load(f)
    polygon_key = config['polygon_key']

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["stocks_db"]
tickers_col = db["tickers"]

# Connect to MySQL database
db = mysql.connector.connect(
    host="localhost",
    user=config['mysql']['user'],
    password=config['mysql']['password'],
    database="stocks"
)

# Create a cursor object
cursor = db.cursor()

# Get ticker names
ticker_names = tickers_col.distinct("ticker")
yesterday = datetime.now() #- timedelta(days=1)
_from = yesterday.strftime('%Y-%m-%d')
# Call the function to receive and load polygon data
data.load_data(db, cursor, polygon_key, ticker_names, _from, _from)
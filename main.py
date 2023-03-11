from pymongo import MongoClient
import pandas as pd

client = MongoClient('mongodb://localhost:27017')

# create db if not exists
mongo_db = client['stocks_db']

# create 'static_data' collection
mongo_collection = mongo_db['static_data']

def get_relevant_users(stock_ticker):
    cur = mongo_collection.find()
    df = pd.DataFrame(list(cur))
    df_relevant_users = df[(df.is_active == 1) & (df.stock_ticker == stock_ticker)]
    return df_relevant_users

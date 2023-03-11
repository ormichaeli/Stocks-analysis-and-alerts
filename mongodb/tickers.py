from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client["stocks_db"]
col = db["tickers"]

tickers_list = [
    {"_id": "1", "ticker": "AAPL", "name": "Apple"},
    {"_id": "2", "ticker": "ABNB", "name": "Airbnb"},
    {"_id": "3", "ticker": "AMZN", "name": "Amazon"},
    {"_id": "4", "ticker": "EBAY", "name": "eBay"},
    {"_id": "5", "ticker": "GOOGL", "name": "Alphabet Inc Class A"},
    {"_id": "6", "ticker": "KO", "name": "Nvidia"},
    {"_id": "7", "ticker": "MCD", "name": "Visa"},
    {"_id": "8", "ticker": "META", "name": "Meta Platforms"},
    {"_id": "9", "ticker": "MSFT", "name": "Microsoft"},
    {"_id": "10", "ticker": "NKE", "name": "Nike"},
    {"_id": "11", "ticker": "NFLX", "name": "Netflix"},
    {"_id": "12", "ticker": "PYPL", "name": "PayPal"},
    {"_id": "13", "ticker": "SBUX", "name": "Starbucks"},
    {"_id": "14", "ticker": "TSLA", "name": "Tesla"},
    {"_id": "15", "ticker": "UBER", "name": "Uber"}
]

col.insert_many(tickers_list)

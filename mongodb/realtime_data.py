import pandas as pd
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client["stocks_db"]
col = db["realtime_data"]

# Fetch data from MongoDB
results = list(col.find())

# Convert data to DataFrame
df = pd.DataFrame(results, columns=["stock_ticker", "current_price", "time"])

# Format current_price column to show two decimal places
df["current_price"] = df["current_price"].map("{:.2f}".format)

print(df)

# retrieve stock data from MongoDB
stock_data = col.aggregate([
    {"$sort": {"stock_ticker": 1, "time": -1}},
    {
        "$group": {
            "_id": "$stock_ticker",
            "current_price": {"$first": "$current_price"},
            "last_time": {"$first": "$time"}
        }
    }
])
for data in stock_data:
    print(data)
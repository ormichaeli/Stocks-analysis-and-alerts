from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")

# Get references to the users and stocks collections
db = client["stocks_db"]
users_col = db["users"]
tickers_col = db["tickers"]

# Define the pipeline for the join
pipeline = [
    {
        "$lookup": {
            "from": "tickers",
            "localField": "stock_name",
            "foreignField": "name",
            "as": "ticker"
        }
    },
    {
        "$project": {
            "_id": 0,
            "email_address": 1,
            "news": 1,
            "ticker.ticker": 1
        }
    }
]

# Perform the join and print the results
results = list(users_col.aggregate(pipeline))
for result in results:
    print(result)
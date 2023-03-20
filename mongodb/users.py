import pandas as pd
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client["stocks_db"]
col = db["users"]

# Fetch data from MongoDB
results = list(col.find())

# Convert data to DataFrame
df = pd.DataFrame(results, columns=["first_name", "last_name", "email_address", "stock_ticker", "price", "news", "is_active"])
print(df)
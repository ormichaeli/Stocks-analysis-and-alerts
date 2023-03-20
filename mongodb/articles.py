import pandas as pd
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client["stocks_db"]
col = db["articles"]

# Fetch data from MongoDB
results = list(col.find())

# Convert data to DataFrame
df = pd.DataFrame(results, columns=["date", "ticker", "published_at", "title", "publisher", "author", "article"])
# Set display option to show all columns
pd.set_option("display.max_columns", None)

max_date = df["date"].max()
max_date_rows = df[df["date"] == max_date]
print(max_date_rows)
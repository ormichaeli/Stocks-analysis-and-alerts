import logging
import requests
import json
from logs.logging_config import write_to_log
from datetime import datetime
from pymongo import MongoClient

dir = '/tmp/pycharm_project_598'

# Get configuration data
with open(f'{dir}/config.json') as f:
    config = json.load(f)
polygon_key = config['polygon_key']

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["stocks_db"]
tickers_col = db["tickers"]
articles_col = db["articles"]

# Get ticker names
ticker_names = tickers_col.distinct("ticker")

# Get articles
for ticker in ticker_names:
    # Make API request and get the results as a JSON object
    ticker = ticker
    date = datetime.today().strftime('%Y-%m-%d')
    url = f'https://api.polygon.io/v2/reference/news?ticker={ticker}' \
          f'&published_utc={date}' \
          f'&order=asc&sort=published_utc&apiKey={polygon_key}'
    response = requests.get(url)
    data = response.json()

    # Create a list to hold the dictionaries for the three articles
    articles = []
    # loop through the articles in the API response and extract the data
    if len(data['results']) > 0:
        for i, article in enumerate(data['results']):
            article_id = ticker + ':' + date + ':' + str(i + 1)
            if articles_col.find_one({"title": article['title'], "published_at": article['published_utc']}) is None:
                article_data = {
                    '_id': article_id,
                    'date': date,
                    'ticker': ticker,
                    'published_at': article['published_utc'],
                    'title': article['title'],
                    'publisher': article['publisher']['name'],
                    'author': article['author'],
                    'article': article['article_url']
                }
                articles.append(article_data)
            else:
                write_to_log('get articles', f'Article {article["title"]} for {ticker} already exists in articles collection',
                             level=logging.ERROR)

        if articles:
            articles_col.insert_many(articles)
            write_to_log('get articles', f'Get {i + 1} articles for {ticker}')
    else:
            write_to_log('get articles', f'No articles about {ticker} were published today', level=logging.ERROR)

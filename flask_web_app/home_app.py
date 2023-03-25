import logging
import json, os
import mysql.connector
from pymongo import MongoClient
from flask import Flask, render_template, request, flash, redirect, url_for
from logs.logging_config import write_to_log

# Get configuration data
with open('/tmp/pycharm_project_301/config.json') as f:
    config = json.load(f)

# Connect to MongoDB database
client = MongoClient('mongodb://localhost:27017/')
db = client['stocks_db']
realtime = db["realtime_data"]
users = db["users"]
tickers = db["tickers"]

# Connect to MySQL database
mysql_conn = mysql.connector.connect(
    host="localhost",
    user=config['mysql']['user'],
    password=config['mysql']['password'],
    database="stocks"
)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY") or "my-secret-key"

@app.route('/')
def index():
    try:
        # retrieve stock data from MongoDB
        stock_data = realtime.aggregate([
            {"$sort": {"stock_ticker": 1, "time": -1}},
            {
                "$group": {
                    "_id": "$stock_ticker",
                    "current_price": {"$first": "$current_price"},
                    "last_time": {"$first": "$time"}
                }
            }
        ])
        stock_data = list(stock_data)
        # add price change based on daily_stocks table
        cursor = mysql_conn.cursor()
        for stock in stock_data:
            ticker = stock['_id']
            # find the maximum date in daily_stocks table
            cursor.execute(f"SELECT MAX(business_day) FROM daily_stocks WHERE ticker='{ticker}'")
            max_date = cursor.fetchone()[0]
            if max_date:
                # get the close price for the maximum date
                cursor.execute(
                    f"SELECT close_price FROM daily_stocks WHERE ticker='{ticker}' AND business_day='{max_date}'")
                close_price = cursor.fetchone()[0]
                # calculate price change
                price_change = round(((stock['current_price'] - close_price) / close_price) * 100, 2)
                stock['price_change'] = price_change
            else:
                stock['price_change'] = None
        # Sort the stock data by ticker symbol in ascending order
        stock_data = sorted(stock_data, key=lambda x: x['_id'])
        # pass stock data to template
        return render_template('home.html', prices=stock_data)

    except Exception as e:
        # log error
        write_to_log('registration form', 'Error occurred while retrieving stock data', level=logging.CRITICAL)
        # return error message to user
        return "Error occurred while retrieving stock data.", 500

@app.route('/register', methods=["GET", "POST"])
def register():
    # Retrieve ticker data from MongoDB
    ticker_data = list(tickers.find({}, {"_id": 0, "ticker": 1, "name": 1}).sort("ticker", 1))
    # format the ticker data
    formatted_ticker_data = [f"{t['name']} ({t['ticker']})" for t in ticker_data]

    if request.method == "POST" and len(request.form) > 1:
        data = request.form.to_dict()
        # Add an indicator for active alert
        data.update({'is_active': 1})
        # Save registration form in MonogoDB
        users.insert_one(data)
        write_to_log('registration form', f'{data["email_address"]} registered for alerts for {data["stock_ticker"]}')
        # Redirect to home page
        return redirect(url_for('index'))
    # pass the formatted ticker data to the template
    return render_template('registration.html', tickers=formatted_ticker_data)

if __name__ == '__main__':
    app.run(debug=True)
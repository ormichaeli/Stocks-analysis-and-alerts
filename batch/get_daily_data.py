import logging
import requests
from datetime import datetime
from logs.logging_config import write_to_log


# Get data from Polygon API
def get_data(polygon_key, ticker_names, _from, to):
    data_for_loading = []
    for ticker in ticker_names:
        # Construct the API URL
        url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}' \
              f'/range/1/day/{_from}/' \
              f'{to}' \
              f'?adjusted=true&sort=asc&apiKey={polygon_key}'
        # Make API request and get the results as a JSON object
        response = requests.get(url)
        data = response.json()
        date = datetime.now().strftime('%Y-%m-%d')
        # If data is returned
        if 'results' in data:
            # Loop over the results and extract relevant data
            for d in data['results']:
                timestamp = d['t'] / 1000.0  # Convert timestamp from milliseconds to seconds
                datetime_obj = datetime.fromtimestamp(timestamp)  # Convert timestamp to datetime object
                date = datetime_obj.date()  # Extract date from datetime object
                stocks_data = [ticker, d['v'], d['vw'], d['o'], d['c'], d['h'], d['l'], date, d['n']]
                data_for_loading.append(stocks_data)
            # Log success
            write_to_log('daily stocks data', f'Get data from the polygon API about {ticker} on {date}')
        else:
            # Log warning if no data is returned
            write_to_log('daily stocks data', f'No data exists for {ticker} on the {date}', level=logging.WARNING)
    # Return the extracted data
    return data_for_loading


# Load data into the database
def load_data(db, cursor, polygon_key, ticker_names, _from, to):
    # Get data from Polygon API
    data_for_loading = get_data(polygon_key, ticker_names, _from, to)
    rowcount = 0
    for data_row in data_for_loading:
        ticker = data_row[0]
        date = data_row[7]
        # Check if the data already exists in the table
        cursor.execute("SELECT id FROM daily_stocks WHERE ticker = %s AND business_day = %s", (ticker, date))
        result = cursor.fetchone()
        if result:
            # Log warning if the data already exists
            write_to_log('daily stocks data', f'Data for {ticker} on {date} already exists in the table',
                         level=logging.WARNING)
        else:
            # Insert the new data into the table
            sql = "INSERT INTO daily_stocks (ticker, volume, volume_weighted, open_price, close_price, highest_price, lowest_price, business_day, transactions_number) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, data_row)
            rowcount += 1
            db.commit()
    # Log the number of rows inserted
    write_to_log('daily stocks data', f'{rowcount} rows inserted on {datetime.today().date()}')
    db.close()
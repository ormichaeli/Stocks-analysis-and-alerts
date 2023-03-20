import logging
import requests
from datetime import datetime
from logs.logging_config import write_to_log


# Get data
def get_data(polygon_key, ticker_names, _from, to):
    data_for_loading = []
    for ticker in ticker_names:
        # Make API request and get the results as a JSON object
        url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}' \
              f'/range/1/' \
              f'day/' \
              f'{_from}/' \
              f'{to}' \
              f'?adjusted=true&sort=asc&apiKey={polygon_key}'
        response = requests.get(url)
        data = response.json()
        date = datetime.now().strftime('%Y-%m-%d')
        if 'results' in data:
            for d in data['results']:
                timestamp = d['t'] / 1000.0  # Divide by 1000 to convert milliseconds to seconds
                datetime_obj = datetime.fromtimestamp(timestamp)  # Convert the timestamp to a datetime object
                date = datetime_obj.date()  # Extract the date from the datetime object
                stocks_data = [ticker, d['v'], d['vw'], d['o'], d['c'], d['h'], d['l'], date, d['n']]
                data_for_loading.append(stocks_data)
            write_to_log('daily stocks data', f'Get data from the polygon API about {ticker} on {date}')
        else:
            write_to_log('daily stocks data', f'No data exists for {ticker} on the {date}', level=logging.WARNING)
    return data_for_loading


def load_data(db, cursor, polygon_key, ticker_names, _from, to):
    data_for_loading = get_data(polygon_key, ticker_names, _from, to)
    rowcount = 0
    for data_row in data_for_loading:
        ticker = data_row[0]
        date = data_row[7]
        cursor.execute("SELECT id FROM daily_stocks WHERE ticker = %s AND business_day = %s", (ticker, date))
        result = cursor.fetchone()
        if result:
            write_to_log('daily stocks data', f'Data for {ticker} on {date} already exists in the table',
                         level=logging.WARNING)
        else:
            # Insert the new data into the table
            sql = "INSERT INTO daily_stocks (ticker, volume, volume_weighted, open_price, close_price, highest_price, lowest_price, business_day, transactions_number) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, data_row)
            rowcount += 1
            db.commit()
    # Print the number of rows inserted
    write_to_log('daily stocks data', f'{rowcount} rows inserted on {datetime.today().date()}')
    db.close()
import utilities
from datetime import datetime
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["stocks_db"]
users_col = db["users"]
articles_col = db["articles"]

# Get users choices data
alerts = users_col.find({'news': 'on', 'is_active': 1},
                        {'first_name': 1, 'email_address': 1, 'stock_ticker': 1, '_id': 0})


# Send email
for user in alerts:
    recipient = user['email_address']
    ticker = user['stock_ticker']
    name = user['first_name'][0].upper() + user['first_name'][1:]
    # Get articles from articles_col
    date = datetime.today().strftime('%Y-%m-%d')
    articles = articles_col.find({'date': date, 'ticker': ticker},
                                 {'title': 1, 'publisher': 1, 'article': 1, 'published_at': 1, 'author': 1, '_id': 0})

    if len(list(articles)) > 0:
        subject = f'Articles about {ticker} stock published today'
        # Create the body of the email
        body = f'<html><head><style>table {{ border-collapse: collapse; }} th, td {{ border: 1px solid #ddd; padding: 12px; }} th {{ text-align: left; background-color: #f2f2f2; }}</style></head><body style="font-family: Arial, sans-serif;">'
        body += f'<p style="font-size: 18px; font-weight: bold; margin-top: 0;">Dear {name},</p>'
        body += f'<p style="font-size: 16px; margin-bottom: 24px;">Below are articles about {ticker} stock published today:</p>'
        body += '<table style="width: 100%; font-size: 14px;">'
        body += '<tr><th style="text-align: left;">Published Time</th><th style="text-align: left;">Article</th><th style="text-align: left;">Publisher</th><th style="text-align: left;">Author</th></tr>'
        for article in articles:
            body += f'<tr><td>{article["published_at"].split("T")[1]}</td><td><a href="{article["article"]}" target="_blank" style="text-decoration: none; color: #0366d6;">{article["title"]}</a></td><td>{article["publisher"]}</td><td>{article["author"]}</td></tr>'
        body += '</table>'
        body += '<p style="font-size: 14px; margin-top: 24px; margin-bottom: 0;">Best regards,</p><p style="font-size: 14px; margin-top: 0; margin-bottom: 0;">Naya Trades Team</p></body></html>'
        # Message for log
        message = f'Articles about {ticker} were sent to {recipient}'
        # Call to send_email function
        utilities.send_email(recipient, subject, body, message)
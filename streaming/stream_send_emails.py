from kafka import KafkaConsumer
import json
from pymongo import MongoClient
from bson.objectid import ObjectId
import utilities


#for the consumer
bootstrapServers = "cnt7-naya-cdh63:9092"
topic4 = 'users_emails'

#to update is_active
client = MongoClient('mongodb://localhost:27017')
mongo_db = client['stocks_db']
mongo_collection = mongo_db['users']

consumer = KafkaConsumer(topic4,bootstrap_servers=bootstrapServers)

for message in consumer:
    request = json.loads(message.value)
    if request:
        request_id = request['_id']
        stock_ticker = request['stock_ticker']
        name = request['first_name'][0].upper() + request['first_name'][1:]
        wanted_price = float(request['price'])
        recipient = request['email_address']

        mongo_collection.update_one({"_id": ObjectId(request_id)},{"$set":{"is_active": 0}})

        subject = f'{stock_ticker} got to the price you wanted!'
        body = f'<html><body><p style="margin-bottom: 10px;"><strong>Hi {name},</strong></p>' \
               f'<p style="margin-top: 0px;">Further to your request,<br>' \
               f'<style="margin-top: 0px; margin-bottom: 10px;">we inform you that the <strong>{stock_ticker}</strong> stock has reached a price of <strong>{wanted_price:.2f}</strong>$</p>' \
               f'<p>To submit another request you are welcome to <a href="http://127.0.0.1:5000">enter our site again</a>, and we will be happy to track the stocks for you :)</p>' \
               f'<p style="margin-top: 10px; margin-bottom: 0px;">Best regards,</p>' \
               f'<p style="margin-top: 0px;">Naya Trades Team</p></body></html>'
        # Message for log
        message = f'Alert about {stock_ticker} was sent to {recipient}'
        # Call to send_email function
        utilities.send_email(recipient, subject, body, message)
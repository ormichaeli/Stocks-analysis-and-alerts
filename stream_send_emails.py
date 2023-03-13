from kafka import KafkaConsumer
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pymongo import MongoClient
from bson.objectid import ObjectId


#for the consumer
bootstrapServers = "cnt7-naya-cdh63:9092"
topic4 = 'users_emails'

#for sending emails
with open('/tmp/pycharm_project_172/config.json') as f:
    config = json.load(f)
    smtp_server = config['email']['smtp_server']
    smtp_port = config['email']['smtp_port']
    sender = config['email']['sender']
    password = config['email']['password']

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

        msg = MIMEMultipart()
        msg['From'] = 'Naya Trades'
        msg['To'] = recipient
        msg['Subject'] = f'{stock_ticker} got to the price you wanted!'
        body = f'Hi {name},\n\n\n \
                Further to your request, we inform you that the {stock_ticker} stock has reached a price of {wanted_price:.2f} dollars.\n \
                To submit another request, you are welcome to enter the form again, and we will be happy to track the stocks for you :)'

        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender, password)

        # Send the email
        text = msg.as_string()
        server.sendmail(msg['From'], msg['To'], text)

        # Close the connection to the SMTP server
        server.quit()
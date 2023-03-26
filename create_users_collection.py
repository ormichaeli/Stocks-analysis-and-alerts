from pymongo import MongoClient

#create a connection
client = MongoClient('mongodb://localhost:27017')

#create db if not exists
mongo_db = client['stocks_db']

#create 'static_data' collection
mongo_collection = mongo_db['users']

users_data = [{'first_name': 'Hanan',
              'last_name': 'Bas',
              'email_address': 'ormichaeli207@gmail.com',
              'stock_ticker': 'AAPL',
               'price': 165,
              'news': 'on',
              'is_active': 1},

              {'first_name': 'Efi',
              'last_name': 'Mor',
              'email_address': 'ormichaeli207@gmail.com',
              'stock_ticker': 'AAPL',
               'price': 165,
               'is_active': 1},

             {'first_name': 'noam',
              'last_name': 'choen',
              'email_address': 'ormichaeli207@gmail.com',
              'stock_ticker': 'TSLA',
               'price': 198,
               'is_active': 1},

             {'first_name': 'avi',
              'last_name': 'micli',
              'email_address': 'ormichaeli207@gmail.com',
              'stock_ticker': 'NFLX',
               'price': 325,
               'is_active': 1},

             {'first_name': 'zeava',
              'last_name': 'micli',
              'email_address': 'ormichaeli207@gmail.com',
              'stock_ticker': 'TSLA',
               'price': 155,
               'is_active': 0},

             {'first_name': 'eden',
              'last_name': 'galam',
              'email_address': 'ormichaeli207@gmail.com',
              'stock_ticker': 'AAPL',
               'price': 165,
               'is_active': 0}
            ]

mongo_collection.insert_many(users_data)

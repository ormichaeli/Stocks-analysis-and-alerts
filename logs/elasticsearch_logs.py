from elasticsearch import Elasticsearch

# create an Elasticsearch client
es_client = Elasticsearch("http://localhost:9200")

# search for the last log message with level INFO
search_results = es_client.search(
    index='logs',
    body={
        'query': {
            'match': {
                'level': 'INFO'
            }
        },
        'sort': {
            'timestamp': {
                'order': 'desc'
            }
        },
        'size': 1
    }
)

# print the last log message
if search_results['hits']['hits']:
    last_message = search_results['hits']['hits'][0]['_source']
    print(last_message['timestamp'], last_message['level'], last_message['message'])
else:
    print('No log messages found')

# delete all documents in the 'logs' index
# es_client.delete_by_query(
#     index='logs',
#     body={
#         'query': {
#             'match_all': {}
#         }
#     }
# )
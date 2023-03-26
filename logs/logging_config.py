import logging
from elasticsearch import Elasticsearch
from datetime import datetime

# Set up Elasticsearch connection
es = Elasticsearch("http://localhost:9200")

# Create logger with the desired format
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(topic)s - %(message)s')

# Get today's date in YYYY-MM-DD format and create a log file path
today = datetime.today().strftime('%Y-%m-%d')
log_file_name = f'/tmp/pycharm_project_355/logs/{today}.log'

# Set up a file handler and stream handler with the formatter
file_handler = logging.FileHandler(log_file_name)
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(stream_handler)


# Define a function for sending logs to Elasticsearch
def send_to_elasticsearch(log_dict):
    # Use the Elasticsearch client to index the log dictionary under the "logs" index
    es.index(index='logs', body=log_dict)


# Write to logger and Elasticsearch
def write_to_log(topic, message, level=logging.INFO):
    # Write the log message to the logger with the specified topic and level
    logger.log(level, message, extra={'topic': topic})

    # Send the log to Elasticsearch
    log_dict = {
        'timestamp': datetime.now(),
        'level': logging.getLevelName(level),
        'topic': topic,
        'message': message
    }
    try:
        send_to_elasticsearch(log_dict)
    except Exception as e:
        logger.log(logging.ERROR, 'Failed to send log message to Elasticsearch: {}'.format(str(e)),
                   extra={'topic': 'Elasticsearch'})
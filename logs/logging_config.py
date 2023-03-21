import datetime
import logging
from elasticsearch import Elasticsearch
from datetime import datetime


# Set up Elasticsearch connection
es = Elasticsearch("http://localhost:9200")

# Create logger with the desired format
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(topic)s - %(message)s')

today = datetime.today().strftime('%Y-%m-%d')
log_file_name = f'/tmp/pycharm_project_598/logs/{today}.log'
file_handler = logging.FileHandler(log_file_name)
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# Define a function for sending logs to Elasticsearch
def send_to_elasticsearch(log_dict):
    es.index(index='logs',  body=log_dict)

# Write to logger and Elasticsearch
def write_to_log(topic, message, level=logging.INFO):
    logger.log(level, message, extra={'topic': topic})

    # Send the log to Elasticsearch
    log_dict = {
        'timestamp': datetime.now(),
        'level': logging.getLevelName(level),
        'topic': topic,
        'message': message
    }
    send_to_elasticsearch(log_dict)
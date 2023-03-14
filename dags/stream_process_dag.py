from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, time, timedelta
from airflow.operators.python_operator import ShortCircuitOperator, PythonOperator
from airflow.operators.sensors import TimeSensor
import pytz

# retries: give the dag a maximum of two retries in case of failure
# retry_delay: tell the DAG to wait 1 minute before retrying
dag = DAG(
    dag_id="streaming_process",
    start_date= datetime(2023, 3, 13),
    schedule_interval='58 8 * * 1-5',      # At 08:58 AM, Monday through Friday
    catchup=False,                        # Defines whether the DAG reruns all DAG runs that were scheduled before today's date.
    tags= ["tutorial"],
    depends_on_past = False,               # Task wont run if its previous task failed = False.
    default_args={
        "owner": 'airflow',
        "retries": 3,
        "retry_delay": timedelta(minutes=0.5)
    }
)

def run_producer_file():
    import requests, json, pytz
    from datetime import datetime
    from kafka import KafkaProducer
    from pymongo import MongoClient
    from time import sleep

    exec(open("/tmp/pycharm_project_172/current_price_producer.py").read())

run_producer = PythonOperator(
    task_id='run_producer',
    python_callable = run_producer_file,
    dag=dag,
)

def run_consumer_kafka_file():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import StringType, StructType, FloatType, IntegerType

    exec(open("/tmp/pycharm_project_172/consumer_kafka.py").read())

run_consumer_kafka = PythonOperator(
    task_id='run_send_to_kafka_again',
    python_callable= run_consumer_kafka_file,
    dag=dag,
)

def run_consumer_hdfs_file():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import StringType, StructType

    exec(open("/tmp/pycharm_project_172/consumer_hdfs.py").read())


run_consumer_hdfs = PythonOperator(
    task_id='run_consumer_hdfs',
    python_callable= run_consumer_hdfs_file,
    dag=dag,
)

def run_consumer_mongo_file():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import StringType, StructType

    exec(open("/tmp/pycharm_project_172/consumer_mongo.py").read())

run_consumer_mongo = PythonOperator(
    task_id='run_consumer_mongo',
    python_callable= run_consumer_mongo_file,
    dag=dag,
)

def run_send_emails_file():
    from kafka import KafkaConsumer
    import json
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from pymongo import MongoClient
    from bson.objectid import ObjectId

    exec(open("/tmp/pycharm_project_172/stream_send_emails.py").read())

run_stream_send_emails = PythonOperator(
    task_id='run_emails_consumer',
    python_callable= run_send_emails_file,
    dag=dag,
)

def stop_dag():
    now_new_york = datetime.now(pytz.timezone('US/Eastern'))
    stop_time = time(hour=16)
    return now_new_york.time() >= stop_time

stop_operator = ShortCircuitOperator(           # Allows a pipeline to continue based on the result of a python_callable.
    task_id='stop_dag',
    python_callable=stop_dag,
    dag=dag,
)

wait_until_16 = TimeSensor(
    task_id='wait_until_16',
    target_time=time(hour=16),
    dag=dag,
)

wait_until_16 >> stop_operator
run_producer
run_consumer_mongo
run_consumer_hdfs
run_consumer_kafka
run_stream_send_emails

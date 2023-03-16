from airflow.models import DAG
from datetime import datetime, time, timedelta
from airflow.operators.python_operator import ShortCircuitOperator, PythonOperator
from airflow.operators.sensors import TimeSensor
import pytz
# import sys, os

# Add the project directory to the Python path
# sys.path.insert(0, '/tmp/pycharm_project_436')

dir = '/tmp/pycharm_project_436'

# retries: give the dag a maximum of two retries in case of failure
# retry_delay: tell the DAG to wait 1 minute before retrying
dag = DAG(
    dag_id="streaming_process",
    start_date= datetime(2023, 3, 16),
    schedule_interval='58 8 * * 1-5',      # At 08:58 AM, Monday through Friday
    catchup=False,                        # Defines whether the DAG reruns all DAG runs that were scheduled before today's date.
    default_args={
        "owner": 'airflow',
        "retries": 3,
        "retry_delay": timedelta(minutes=0.5)
    }
)

def run_producer_file():
    exec(open(f"{dir}/current_price_producer.py").read())

run_producer = PythonOperator(
    task_id='run_producer',
    python_callable = run_producer_file,
    dag=dag,
)

def run_consumer_kafka_file():
    exec(open(f"{dir}/consumer_kafka.py").read())

run_consumer_kafka = PythonOperator(
    task_id='run_send_to_kafka_again',
    python_callable= run_consumer_kafka_file,
    dag=dag,
)

def run_consumer_hdfs_file():
    exec(open(f"{dir}/consumer_hdfs.py").read())


run_consumer_hdfs = PythonOperator(
    task_id='run_consumer_hdfs',
    python_callable= run_consumer_hdfs_file,
    dag=dag,
)

def run_consumer_mongo_file():
    exec(open(f"{dir}/consumer_mongo.py").read())

run_consumer_mongo = PythonOperator(
    task_id='run_consumer_mongo',
    python_callable= run_consumer_mongo_file,
    dag=dag,
)

def run_send_emails_file():
    exec(open(f"{dir}/stream_send_emails.py").read())

run_stream_send_emails = PythonOperator(
    task_id='run_emails_consumer',
    python_callable= run_send_emails_file,
    dag=dag,
)

def stop_dag():
    now_new_york = datetime.now(pytz.timezone('US/Eastern'))
    stop_time = time(hour=16)
    return now_new_york.time() <= stop_time        #to change the condition !!!

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

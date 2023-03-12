from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, time, timedelta
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.operators.sensors import TimeSensor

# retries: give the dag a maximum of two retries in case of failure
# retry_delay: tell the DAG to wait 1 minute before retrying
dag = DAG(
    dag_id="streaming_process",
    start_date= datetime(2023, 3, 12),
    schedule_interval='0 9 * * 1-5',      # At 09:00 AM, Monday through Friday
    catchup=False,                        # Defines whether the DAG reruns all DAG runs that were scheduled before today's date.
    tags= ["tutorial"],
    default_args={
        "owner": 'airflow',
        "retries": 3,
        "retry_delay": timedelta(minutes=1)
    }
)

run_producer = BashOperator(
    task_id='run_producer',
    bash_command='python /tmp/pycharm_project_35/current_price_producer.py',
    dag=dag,
)

run_consumer_s3 = BashOperator(
    task_id='run_consumer_s3',
    bash_command='python /tmp/pycharm_project_35/consumer_s3.py',
    dag=dag,
)

run_consumer_s3 >> run_producer

run_consumer_mongo = BashOperator(
    task_id='run_consumer_mongo',
    bash_command='python /tmp/pycharm_project_35/consumer_mongo.py',
    dag=dag,
)

run_consumer_mongo >> run_producer

run_consumer_kafka = BashOperator(
    task_id='run_send_to_kafka_again',
    bash_command='python /tmp/pycharm_project_35/consumer_kafka.py',
    dag=dag,
)

run_consumer_kafka >> run_producer

run_stream_send_emails = BashOperator(
    task_id='run_emails_consumer',
    bash_command='python /tmp/pycharm_project_35/stream_send_emails.py',
    dag=dag,
)

run_stream_send_emails >> run_consumer_kafka

def stop_dag():
    now = datetime.now()
    stop_time = time(hour=16)
    return now.time() >= stop_time

stop_operator = ShortCircuitOperator(
    task_id='stop_dag',
    python_callable=stop_dag,
    dag=dag,
)

stop_operator >> run_stream_send_emails

wait_until_16 = TimeSensor(
    task_id='wait_until_16',
    target_time=time(hour=16),
    dag=dag,
)

wait_until_16 >> stop_operator
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, time, timedelta
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.operators.sensors import TimeSensor
import pytz

# retries: give the dag a maximum of two retries in case of failure
# retry_delay: tell the DAG to wait 1 minute before retrying
dag = DAG(
    dag_id="streaming_process",
    start_date= datetime(2023, 3, 12),
    schedule_interval='58 8 * * 1-5',      # At 08:58 AM, Monday through Friday
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
    bash_command='python /tmp/pycharm_project_172/current_price_producer.py',
    dag=dag,
)

run_consumer_s3 = BashOperator(
    task_id='run_consumer_s3',
    bash_command='python /tmp/pycharm_project_172/consumer_hdfs.py',
    dag=dag,
)

run_consumer_mongo = BashOperator(
    task_id='run_consumer_mongo',
    bash_command='python /tmp/pycharm_project_172/consumer_mongo.py',
    dag=dag,
)

run_consumer_kafka = BashOperator(
    task_id='run_send_to_kafka_again',
    bash_command='python /tmp/pycharm_project_172/consumer_kafka.py',
    dag=dag,
)

run_stream_send_emails = BashOperator(
    task_id='run_emails_consumer',
    bash_command='python /tmp/pycharm_project_172/stream_send_emails.py',
    dag=dag,
)

# trigger_stream_send_emails = TriggerDagRunOperator(
#     task_id='trigger_stream_send_emails',
#     trigger_dag_id='streaming_process',
#     dag=dag
# )
#
# trigger_consumer_kafka = TriggerDagRunOperator(
#     task_id='trigger_consumer_kafka',
#     trigger_dag_id='streaming_process',
#     dag=dag
# )
#
# trigger_consumer_s3 = TriggerDagRunOperator(
#     task_id='trigger_consumer_s3',
#     trigger_dag_id='streaming_process',
#     dag=dag
# )
#
# trigger_consumer_mongo = TriggerDagRunOperator(
#     task_id='trigger_consumer_mongo',
#     trigger_dag_id='streaming_process',
#     dag=dag
# )
#
# trigger_producer = TriggerDagRunOperator(
#     task_id='trigger_producer',
#     trigger_dag_id='streaming_process',
#     dag=dag
# )

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
#
# trigger_wait_operator = TriggerDagRunOperator(
#     task_id='trigger_wait_operator',
#     trigger_dag_id='streaming_process',
#     dag=dag
# )
#
# trigger_stream_send_emails >> run_stream_send_emails >> \
# trigger_consumer_kafka >> run_consumer_kafka >> \
# trigger_consumer_mongo >> run_consumer_mongo >> \
# trigger_consumer_s3 >> run_consumer_s3 >> \
# trigger_producer >> run_producer >> \
# trigger_wait_operator >> wait_until_16 >> stop_operator

stop_operator
wait_until_16
run_producer
run_consumer_mongo
run_consumer_s3
run_consumer_kafka
run_stream_send_emails

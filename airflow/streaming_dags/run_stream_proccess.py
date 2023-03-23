import sys
from airflow.models import DAG
from datetime import datetime, time, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

PROJECT_DIR  = '/tmp/pycharm_project_681'


dag = DAG(
    dag_id="run_stream_process",
    default_args={
        "owner": 'Airflow'
    },
    start_date= datetime(2023, 3, 23),
    schedule_interval='58 8 * * 1-5',        # At 08:58 AM, Monday through Friday
    tags=['stocks_analysis_and_alerts_final_project']
)

def run_producer():
    import subprocess
    subprocess.run(['python', f'{PROJECT_DIR}/streaming/current_price_producer.py'])

def run_send_to_kafka():
    import subprocess
    subprocess.run(['python', f'{PROJECT_DIR}/streaming/consumer_kafka.py'])

def run_send_to_hdfs():
    import subprocess
    subprocess.run(['python', f'{PROJECT_DIR}/streaming/consumer_hdfs.py'])

def run_send_to_mongo():
    import subprocess
    subprocess.run(['python', f'{PROJECT_DIR}/streaming/consumer_mongo.py'])

def run_send_emails():
    import subprocess
    subprocess.run(['python', f'{PROJECT_DIR}/streaming/stream_send_emails.py'])

run_producer = PythonOperator(
    task_id='run_producer',
    python_callable= run_producer,
    dag=dag
)

run_send_to_kafka = PythonOperator(
    task_id='run_send_to_kafka',
    python_callable= run_send_to_kafka,
    dag=dag
)

run_send_to_hdfs = PythonOperator(
    task_id='run_send_to_hdfs',
    python_callable= run_send_to_hdfs,
    dag=dag
)

run_send_to_mongo = PythonOperator(
    task_id='run_send_to_mongo',
    python_callable= run_send_to_mongo,
    dag=dag
)

run_send_emails = PythonOperator(
    task_id='run_send_emails',
    python_callable= run_send_emails,
    dag=dag
)

def stop_dag_if_after_16pm():
    now_new_york = datetime.now(pytz.timezone('US/Eastern'))
    if now_new_york.hour >= 16:
        raise ValueError('DAG stop running after 16:00')

stop_operator = PythonOperator(
    task_id='stop_dag',
    python_callable= lambda: print("not 16 yet"),
    on_failure_callback= stop_dag_if_after_16pm,
    dag=dag,
)

dummy_task = DummyOperator(task_id='dummy_task', dag=dag)

dummy_task >> run_send_emails
dummy_task >> run_send_to_kafka
dummy_task >> run_send_to_hdfs
dummy_task >> run_send_to_mongo
dummy_task >> run_producer
dummy_task >> stop_operator

if __name__ == "__main__":
    dag.cli()
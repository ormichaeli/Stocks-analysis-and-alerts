from airflow.models import DAG
from datetime import datetime, time, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator


dir = '/tmp/pycharm_project_696'

# retries: give the dag a maximum of two retries in case of failure
# retry_delay: tell the DAG to wait 1 minute before retrying
dag = DAG(
    dag_id="run_send_to_kafka",
    default_args={
        "owner": 'Airflow'
    },
    start_date= datetime(2023, 3, 18),
    schedule_interval='58 8 * * 1-5',        # At 08:58 AM, Monday through Friday
    tags=['stocks_analysis_and_alerts_final_project']
)

run_consumer_kafka = BashOperator(
    task_id='run_consumer_kafka',
    bash_command= f"python {dir}/consumer_kafka.py",
    dag=dag,
)

dummy_task = DummyOperator(task_id='dummy_task', dag=dag)

dummy_task >> run_consumer_kafka

if __name__ == "__main__":
    dag.cli()
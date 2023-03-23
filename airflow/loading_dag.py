from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Add the path to the project directory to the Python path
project_dir = '/tmp/pycharm_project_488'

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 3, 18)
}

dag = DAG(
    'daily_loading',
    default_args=default_args,
    description='Loads daily data into MySQL database',
    schedule_interval='0 0 * * 1-5',  # Run on weekdays at 00:00
)

load_data_task = BashOperator(
    task_id='load_data',
    bash_command= f'python {project_dir}/batch/daily_loading.py',
    dag=dag
)

dummy_task = DummyOperator(task_id='dummy_task', dag=dag)

dummy_task >> load_data_task

if __name__ == "__main__":
    dag.cli()
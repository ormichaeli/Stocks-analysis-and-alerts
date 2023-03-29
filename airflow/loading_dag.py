import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Set project directory path
project_dir = '/tmp/pycharm_project_731'
# Add the project directory to system path so modules can be imported
sys.path.insert(0, project_dir)


# Function to run a Python script using subprocess module
def load_data_func(script_path):
    import os
    import subprocess
    os.environ['PYTHONPATH'] = f'{project_dir}:{os.environ.get("PYTHONPATH", "")}'
    subprocess.run(['python', script_path])


# Default DAG arguments
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 28)
}

# Define the DAG
dag = DAG(
    dag_id='loading_dag',
    default_args=default_args,
    description='Loads daily data into MySQL database',
    schedule_interval='45 20 * * 1-5',  # Run on weekdays at 20:45 utc (23:45 israel time)
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data_func,
    op_kwargs={'script_path': f'{project_dir}/batch/daily_loading.py'},
    dag=dag
)

load_data_task
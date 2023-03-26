import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Set project directory path
project_dir = '/tmp/pycharm_project_355'
# Add the project directory to system path so modules can be imported
sys.path.insert(0, project_dir)


# Function to run a Python script using subprocess module
def run_python_script(script_path):
    import os
    import subprocess
    os.environ['PYTHONPATH'] = f'{project_dir}:{os.environ.get("PYTHONPATH", "")}'
    subprocess.run(['python', script_path])


# Default DAG arguments
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 5, 22, 0)
}

# Define the DAG
dag = DAG(
    dag_id='articles_dag',
    default_args=default_args,
    description='Get and send articles every day at 22:00 (Israel time)',
    schedule_interval='0 19 * * *',
)

# Define the task to get articles
get_articles_task = PythonOperator(
    task_id='get_articles',
    python_callable=run_python_script,
    op_kwargs={'script_path': f'{project_dir}/articles/get_articles.py'},
    dag=dag
)

# Define the task to send articles
send_articles_task = PythonOperator(
    task_id='send_articles',
    python_callable=run_python_script,
    op_kwargs={'script_path': f'{project_dir}/articles/send_articles.py'},
    dag=dag
)

# Set task dependencies
get_articles_task >> send_articles_task
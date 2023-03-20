import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


PROJECT_DIR = '/tmp/pycharm_project_696'

sys.path.insert(0, PROJECT_DIR)

# def get_articles():
#     import subprocess
#     subprocess.run(['python', f'{PROJECT_DIR}/articles/get_articles.py'])


def send_articles():
    exec(compile(open(f'{PROJECT_DIR}/articles/send_articles.py').read(),
                 f'{PROJECT_DIR}/articles/send_articles.py', 'exec'))


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 5, 22, 0)
}

dag = DAG(
    dag_id='articles_dag',
    default_args=default_args,
    description='Get and send articles every night at 22:00',
    schedule_interval='0 22 * * *',
)

get_articles = PythonOperator(
    task_id='get_articles',
    python_callable=get_articles,
    dag=dag
)

send_articles = PythonOperator(
    task_id='send_articles',
    python_callable=send_articles,
    dag=dag
)

get_articles >> send_articles
send_articles
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# retries: give the dag a maximum of two retries in case of failure
# retry_delay: tell the DAG to wait 1 minute before retrying
dag = DAG(
    dag_id="streaming_process",
    schedule_interval='',
    catchup=False,
    tags= ["tutorial"],
    default_args={
        "owner": 'airflow',
        "retries": 3,
        "retry_delay": timedelta(minutes=1)
    }
)
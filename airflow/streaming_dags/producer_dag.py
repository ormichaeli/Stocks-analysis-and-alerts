from airflow.models import DAG
from datetime import datetime, time, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator


dir = '/tmp/pycharm_project_598'

# retries: give the dag a maximum of two retries in case of failure
# retry_delay: tell the DAG to wait 1 minute before retrying
dag = DAG(
    dag_id="run_producer",
    default_args={
        "owner": 'Airflow'
    },
    start_date= datetime(2023, 3, 18),
    schedule_interval='59 8 * * 1-5',        # At 08:59 AM, Monday through Friday
    tags=['stocks_analysis_and_alerts_final_project']
)

run_producer = BashOperator(
    task_id='run_producer',
    bash_command= f"python {dir}/streaming/current_price_producer.py",
    dag=dag,
)

dummy_task = DummyOperator(task_id='dummy_task', dag=dag)

dummy_task >> run_producer

if __name__ == "__main__":
    dag.cli()
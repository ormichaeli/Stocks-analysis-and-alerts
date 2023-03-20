from airflow.models import DAG
from datetime import datetime, time, timedelta
from airflow.operators.python_operator import PythonOperator
import pytz
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator


dir = '/tmp/pycharm_project_696'

# retries: give the dag a maximum of two retries in case of failure
# retry_delay: tell the DAG to wait 1 minute before retrying
dag = DAG(
    dag_id="streaming_process",
    default_args={
        "owner": 'Airflow'
    },
    start_date= datetime(2023, 3, 16),
    schedule_interval='58 8 * * 1-5',        # At 08:58 AM, Monday through Friday
    tags=['stocks_analysis_and_alerts_final_project']
)

# def stop_dag_if_after_16pm():
#     now_new_york = datetime.now(pytz.timezone('US/Eastern'))
#     if now_new_york.hour >= 16:
#         raise ValueError('DAG stop running after 16:00')
#
# stop_operator = PythonOperator(
#     task_id='stop_dag',
#     python_callable= lambda: print("stop_task"),
#     on_failure_callback= stop_dag_if_after_16pm,
#     dag=dag,
# )

# dummy_task >> run_stream_send_emails
# dummy_task >> stop_operator

if __name__ == "__main__":
    dag.cli()
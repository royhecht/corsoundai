from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from my_funcs.trigger_funcs import read_s3_file, trigger_worker_dag

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=0.25),
}

with DAG('trigger_process_dag',
         default_args=default_args,
         description='A simple example DAG',
         schedule_interval=None) as dag:

    read_s3_file_task = PythonOperator(
        task_id='read_s3_file',
        python_callable=read_s3_file,
        provide_context=True,
    )

    trigger_worker_dag_task = PythonOperator(
        task_id='trigger_worker_dag',
        python_callable=trigger_worker_dag,
        provide_context=True,
    )

    worker_completion_sensor = ExternalTaskSensor(
        task_id='worker_completion_sensor',
        external_dag_id='youtube_processing_dag',
        external_task_id='store_to_s3',
        mode='reschedule',
        timeout=3600,
        poke_interval=60,
    )

    re_trigger_self = TriggerDagRunOperator(
        task_id='re_trigger_self',
        trigger_dag_id='trigger_dag',
        wait_for_completion=False,
    )

    read_s3_file_task >> trigger_worker_dag_task >> worker_completion_sensor >> re_trigger_self

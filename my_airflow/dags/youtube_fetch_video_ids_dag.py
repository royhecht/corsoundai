from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from my_funcs.extract_funcs import fetch_youtube_video_ids, store_video_ids_in_s3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG('youtube_fetch_video_ids_dag',
         default_args=default_args,
         description='A simple example DAG',
         schedule_interval=None) as dag:

    fetch_video_ids_task = PythonOperator(
        task_id='fetch_video_ids',
        python_callable=fetch_youtube_video_ids,
        dag=dag
    )

    store_ids_to_s3_task = PythonOperator(
        task_id='store_ids_to_s3',
        python_callable=store_video_ids_in_s3,
        dag=dag
    )

    fetch_video_ids_task >> store_ids_to_s3_task

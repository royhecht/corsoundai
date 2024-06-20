from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from my_funcs.process_funcs import download_video, extract_audio, perform_vad, store_speech_to_s3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=0.25),
}

with DAG('youtube_processing_dag',
         default_args=default_args,
         description='A simple example DAG',
         schedule_interval=None,
         max_active_runs=3) as dag:

    download_from_youtube_task = PythonOperator(
        task_id='download_from_youtube',
        python_callable=download_video,
        dag=dag
    )

    extract_audio_from_video_task = PythonOperator(
        task_id='extract_audio_from_video',
        python_callable=extract_audio,
        dag=dag
    )

    preform_vad_task = PythonOperator(
        task_id='preform_vad',
        python_callable=perform_vad,
        dag=dag
    )

    store_to_s3_task = PythonOperator(
        task_id='store_to_s3',
        python_callable=store_speech_to_s3,
        dag=dag
    )

    download_from_youtube_task >> extract_audio_from_video_task >> preform_vad_task >> store_to_s3_task

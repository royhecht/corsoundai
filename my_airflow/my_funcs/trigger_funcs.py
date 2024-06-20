import json
import logging

from airflow.operators.dagrun_operator import TriggerDagRunOperator

from aws_login import get_s3_client
from .constants import BUCKET_NAME


def read_s3_file(**kwargs):
    s3 = get_s3_client()
    prefix = 'video_ids/'
    files = [v['Key'] for v in s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)['Contents']]
    logging.info(files)
    if not files:
        msg = "No files found in S3 bucket"
        logging.info(msg)
        raise ValueError(msg)
    file_content = s3.get_object(Bucket=BUCKET_NAME, Key=files[0])
    s3.delete_object(Bucket=BUCKET_NAME, Key=files[0])
    return json.loads(file_content['Body'].read().decode('utf-8'))


def trigger_worker_dag(**kwargs):
    ti = kwargs['ti']
    ids = ti.xcom_pull(task_ids='read_s3_file')

    for id_ in ids: #[1:3] restriction low on resourses
        trigger = TriggerDagRunOperator(
            task_id=f'trigger_worker_dag_{id_}',
            trigger_dag_id='youtube_processing_dag',
            conf={'video_id': id_},
            wait_for_completion=False,
            dag=kwargs['dag']
        )
        trigger.execute(context=kwargs)


def check_worker_completion(**kwargs):
    pass

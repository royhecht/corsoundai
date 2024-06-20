import json
import logging
from datetime import datetime

import googleapiclient.discovery

from my_funcs.aws_login import get_s3_client
from my_funcs.constants import MAX_RESULTS, API_KEY, BUCKET_NAME


def fetch_youtube_video_ids(**kwargs):
    query = kwargs.get('params', {}).get('query', {})
    youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=API_KEY)
    request = youtube.search().list(
        part="id",
        q=query,
        type="video",
        maxResults=MAX_RESULTS
    )
    response = request.execute()
    return [item['id']['videoId'] for item in response['items']]


def store_video_ids_in_s3(**kwargs):
    ti = kwargs['ti']
    video_ids = ti.xcom_pull(task_ids='fetch_video_ids')
    logging.info(video_ids)
    s3 = get_s3_client()
    s3_path = f'video_ids/{datetime.now().strftime("%Y%m%d%H%M%S")}.json'
    logging.info(s3_path)
    s3.put_object(Bucket=BUCKET_NAME, Key=s3_path, Body=json.dumps(video_ids))

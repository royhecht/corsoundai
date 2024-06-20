import logging

from moviepy.editor import VideoFileClip
from pydub import AudioSegment
from yt_dlp import YoutubeDL

from constants import BUCKET_NAME
from .aws_login import get_s3_client
from .vad import VADProcessor


def download_video(**kwargs):
    video_id = kwargs.get('params', {}).get('video_id', {})
    ydl_opts = {
        'format': 'best',
        'outtmpl': f'/tmp/{video_id}.mp4'
    }
    with YoutubeDL(ydl_opts) as ydl:
        ydl.download([f'https://www.youtube.com/watch?v={video_id}'])
    return f'/tmp/{video_id}.mp4'


def extract_audio(**kwargs):
    logging.info(f"kwargs: {kwargs}")
    ti = kwargs['ti']
    video_path = ti.xcom_pull(task_ids='download_from_youtube')
    logging.info(f"Retrieved video_path from XCom: {video_path}")

    audio_path = video_path.replace('.mp4', '.wav')
    _convert_video_to_audio(video_path, audio_path)
    return audio_path


def _convert_video_to_audio(video_path, audio_path):
    video_clip = VideoFileClip(video_path)
    audio_clip = video_clip.audio
    audio_clip.write_audiofile(audio_path)
    audio_clip.close()
    video_clip.close()


def perform_vad(**kwargs):
    ti = kwargs['ti']
    audio_path = ti.xcom_pull(task_ids='extract_audio_from_video')
    logging.info(f"the audio path is:{audio_path}")
    return VADProcessor(audio_path=audio_path).perform_vad()




def store_speech_to_s3(**kwargs):
    ti = kwargs['ti']
    audio_path = ti.xcom_pull(task_ids='extract_audio_from_video')
    speech_time_ranges = ti.xcom_pull(task_ids='preform_vad')
    video_id = kwargs.get('params', {}).get('video_id', {})
    audio_path.replace('.wav', '.mp4') #file deleted after first use in tmp
    logging.info(f'audio_path:{audio_path},speech_time_ranges: {speech_time_ranges},video_id: {video_id}')
    for i, time_range in enumerate(speech_time_ranges):
        start_ms, end_ms = time_range
        file_name = f'speech_segment_{video_id}_{i}.wav'
        _cut_audio(audio_path, start_ms, end_ms, file_name)
        upload_to_s3(file_name)


def _cut_audio(input_audio_path, start_ms, end_ms, file_name):
    audio = AudioSegment.from_file(input_audio_path)
    segment = audio[start_ms:end_ms]
    segment.export(f'tmp/{file_name}', format="wav")


def upload_to_s3(file_name):
    s3_client = get_s3_client()
    try:
        s3_client.upload_file(f'tmp/{file_name}', BUCKET_NAME, file_name)
        return True
    except Exception as e:
        print(f"Error uploading file to S3: {e}")
        return False

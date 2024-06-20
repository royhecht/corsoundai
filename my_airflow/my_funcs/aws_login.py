import boto3

from my_funcs.constants import REGION, ACCESS_KEY, SECRET_KEY


def get_s3_client():
    session = boto3.Session(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION
    )

    return session.client('s3')

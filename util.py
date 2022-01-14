from datetime import datetime as dt
from datetime import timedelta as td
from upload import get_client
from botocore.errorfactory import ClientError
import os

def get_prev_file_name(bucket, file_prefix, bookmark_file):
    s3_client = get_client()
    try:
        bookmark_file = s3_client.get_object(
            Bucket=bucket,
            Key=f"{file_prefix}/{bookmark_file}",
        )
        prev_file = bookmark_file["Body"].read().decode("UTF-8")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            prev_file = os.environ.get("BASELINE_FILE")
        else:
            raise

    return prev_file


def get_next_file_name(prev_file):
    dt_part = prev_file.split(".")[0]
    next_file = f"{dt.strftime(dt.strptime(dt_part, '%Y-%M-%d-%H') + td(hours=1), '%Y-%M-%d-%H')}.json.gz"
    if int(next_file[11:13]) < 10:
        temp = next_file
        next_file = temp[:11] + temp[12:]

    return next_file


def upload_bookmark(bucket, file_prefix, bookmark_file, bookmark_content):
    s3_client = get_client()
    upload_res = s3_client.put_object(
        Bucket=bucket,
        Key=f'{file_prefix}/{bookmark_file}',
        Body=bookmark_content.encode('utf-8')
    )
    return upload_res







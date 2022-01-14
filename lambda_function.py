import json
import os
import boto3
from download import *
from upload import *
from util import *


def lambda_handler(event, context):
    bucket = os.environ.get("BUCKET_NAME")
    bookmark_file = os.environ.get("BOOKMARK_FILE")
    baseline_file = os.environ.get("BASELINE_FILE")
    file_prefix = os.environ.get("FILE_PREFIX")
    for i in range (24):
        prev_file = get_prev_file_name(bucket, file_prefix, bookmark_file)
        file_name = get_next_file_name(prev_file)
        download_res = download_file(file_name)

        #base-condition
        if download_res.status_code != 200:
            print("file not found")
            break

        upload_res = upload_s3(
            download_res.content,
            bucket,
            f'{file_prefix}/{file_name}'
        )

        print(f'File {file_name} successfully processed')
        upload_bookmark(bucket,file_prefix,bookmark_file,file_name)










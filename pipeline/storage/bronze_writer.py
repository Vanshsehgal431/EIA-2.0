import json
from datetime import datetime

import boto3

from config.settings import AWS_REGION, S3_BUCKET

s3 = boto3.client("s3", region_name=AWS_REGION)


def save_raw_payload(payload: dict, source: str):
    """
    Save raw JSON payload to S3 Bronze Layer
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    key = f"bronze/{source}/{timestamp}.json"

    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(payload))

    return key

"""
Punto de entrada para la capa de procesamiento
"""

import json
import os
from datetime import datetime

import boto3


def run():
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

    data = {
        "layer": "processing",
        "timestamp": datetime.utcnow().isoformat(),
        "test": "cleaned data",
    }

    key = f"processed/test_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    s3.put_object(Bucket="hackernews-datalake", Key=key, Body=json.dumps(data))


if __name__ == "__main__":
    run()

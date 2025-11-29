import boto3
from botocore.config import Config
from typing import Optional
import os
from .config import settings

_client = None

def get_s3_client():
    global _client
    if _client is None:
        cfg = Config(signature_version='s3v4', retries={'max_attempts': 5})
        _client = boto3.client(
            "s3",
            endpoint_url=settings.MINIO_ENDPOINT,
            aws_access_key_id=settings.MINIO_ACCESS_KEY,
            aws_secret_access_key=settings.MINIO_SECRET_KEY,
            config=cfg,
            use_ssl=settings.MINIO_SECURE,
        )
    return _client

def upload_file(local_path: str, bucket: str, key: str) -> None:
    s3 = get_s3_client()
    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
    s3.upload_file(local_path, bucket, key)

def download_file(bucket: str, key: str, local_path: str) -> None:
    s3 = get_s3_client()
    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
    s3.download_file(bucket, key, local_path)

def generate_presigned_url(bucket: str, key: str, expires_in: int = 3600) -> str:
    s3 = get_s3_client()
    return s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': key}, ExpiresIn=expires_in)

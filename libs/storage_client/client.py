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

def list_objects(bucket: str, prefix: str = "") -> list[str]:
    """
    List all object keys in a given bucket under a specific prefix.

    Args:
        bucket (str): Name of the S3 bucket.
        prefix (str): Prefix inside the bucket (folder-like path).

    Returns:
        list[str]: A list of object keys.
    """
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")

    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    return keys


def delete_file(bucket: str, key: str) -> None:
    """
    Удаляет один объект (файл) из бакета.

    Args:
        bucket (str): Название бакета.
        key (str): Ключ объекта (путь к файлу в бакете).
    """
    s3 = get_s3_client()
    s3.delete_object(Bucket=bucket, Key=key)


def delete_directory(bucket: str, prefix: str) -> None:
    """
    Удаляет все объекты, находящиеся «внутри директории» (т.е. с заданным префиксом).
    В S3 директорий нет, поэтому удаляем всё, что начинается с prefix.
    Если prefix заканчивается на '/', это стандартный способ обозначения папки.

    Args:
        bucket (str): Название бакета.
        prefix (str): Префикс (виртуальная «папка»), например 'tasks/123/' или 'tasks/123'.
    """
    s3 = get_s3_client()

    if not prefix.endswith("/"):
        prefix = prefix + "/"

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    objects_to_delete = []
    for page in pages:
        for obj in page.get("Contents", []):
            objects_to_delete.append({"Key": obj["Key"]})

        if len(objects_to_delete) >= 1000:
            s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": objects_to_delete, "Quiet": True}
            )
            objects_to_delete = []

    if objects_to_delete:
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": objects_to_delete, "Quiet": True}
        )
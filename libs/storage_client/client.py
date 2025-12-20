import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from typing import Optional
import os
from pathlib import Path

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


def _build_key(project_name: Optional[str], filename: str) -> str:
    """
    Вспомогательная функция для формирования ключа в бакете.
    Если project_name есть — файл кладётся в {project_name}/input_file/{filename}
    Если нет — просто {filename}
    """
    if project_name:
        # Убираем возможные слеши в начале/конце, чтобы не было лишних //
        project_name = project_name.strip("/")
        filename = filename.lstrip("/")
        return f"{project_name}/input_file/{filename}"
    else:
        return filename.lstrip("/")


def upload_file(
    local_path: str,
    bucket: str,
    project_name: str,
    key: str,
) -> str:
    """
    Загружает файл по полному ключу key.
    project_name игнорируется, так как key уже полный (для совместимости с новым API).
    """
    s3 = get_s3_client()

    # Если key не задан — берём имя файла из local_path
    if not key:
        key = Path(local_path).name

    # Если project_name указан — добавляем его как префикс (для старой совместимости)
    if project_name:
        project_name = project_name.strip("/")
        key = f"{project_name}/{key.lstrip('/')}"

    # Загружаем
    s3.upload_file(local_path, bucket, key)

    return key

def upload_process_file(
    local_path: str,
    bucket: str,
    project_name: str,
    filename: Optional[str] = None,
) -> str:
    """
    Загружает файл в бакет с автоматическим созданием структуры папок.

    Args:
        local_path (str): Путь к локальному файлу.
        bucket (str): Название бакета.
        project_name (str): Название проекта — будет создана папка с этим именем.
        filename (Optional[str]): Имя файла в хранилище.
                                  Если None — берётся basename из local_path.

    Returns:
        str: Финальный ключ (key) объекта в бакете.
    """
    s3 = get_s3_client()

    if filename is None:
        filename = Path(local_path).name

    key = _build_key(project_name, filename)

    # Локальные директории создаём на всякий случай (хотя для загрузки не обязательно)
    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)

    s3.upload_file(local_path, bucket, key)

    return key  # удобно возвращать, чтобы потом можно было использовать

def download_file(
    bucket: str,
    key: str,
    local_path: str,
    project_name: Optional[str] = None,
) -> None:
    """
    Скачивает файл. Если указан project_name — ключ будет построен автоматически.
    """
    s3 = get_s3_client()

    if project_name:
        # Если передаём project_name, но key — это только имя файла
        filename = Path(key).name if os.path.sep in key or '/' in key else key
        key = _build_key(project_name, filename)

    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
    s3.download_file(bucket, key, local_path)


def file_exists(
    bucket: str,
    key: str,
    project_name: Optional[str] = None,
) -> bool:
    """
    Проверяет существование объекта в бакете.

    Args:
        bucket (str): Название бакета.
        key (str): Ключ объекта (имя файла или полный путь).
        project_name (Optional[str]): Если указано — проверка идёт внутри
                                      {project_name}/input_file/{key}

    Returns:
        bool: True, если объект существует.
    """
    s3 = get_s3_client()

    if project_name:
        filename = Path(key).name if os.path.sep in key or '/' in key else key
        key = _build_key(project_name, filename)

    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] in ('404', '403'):
            return False
        # Другие ошибки (например, нет доступа к бакету) — пробрасываем
        raise


def generate_presigned_url(
    bucket: str,
    key: str,
    project_name: Optional[str] = None,
    expires_in: int = 3600,
) -> str:
    s3 = get_s3_client()

    if project_name:
        filename = Path(key).name if os.path.sep in key or '/' in key else key
        key = _build_key(project_name, filename)

    return s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket, 'Key': key},
        ExpiresIn=expires_in
    )


# Остальные функции (list_objects, delete_file, delete_directory) можно оставить как есть,
# либо тоже адаптировать под project_name при необходимости.
# Пока оставляю без изменений для совместимости.

def list_objects(bucket: str, prefix: str = "") -> list[str]:
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def delete_file(bucket: str, key: str, project_name: Optional[str] = None) -> None:
    s3 = get_s3_client()
    if project_name:
        filename = Path(key).name if os.path.sep in key or '/' in key else key
        key = _build_key(project_name, filename)
    s3.delete_object(Bucket=bucket, Key=key)


def delete_directory(bucket: str, prefix: str) -> None:
    s3 = get_s3_client()
    if not prefix.endswith("/"):
        prefix = prefix + "/"
    paginator = s3.get_paginator("list_objects_v2")
    objects_to_delete = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
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

def move_file(
    bucket: str,
    source_key: str,
    destination_key: str,
    project_name: Optional[str] = None,
    delete_source: bool = True
) -> str:
    """
    Перемещает или переименовывает файл в хранилище MinIO.
    
    Args:
        bucket (str): Название бакета.
        source_key (str): Исходный ключ (путь) файла.
        destination_key (str): Целевой ключ (путь) файла.
        project_name (Optional[str]): Если указан, применяется к обоим ключам.
        delete_source (bool): Удалять ли исходный файл после копирования (True по умолчанию).
    
    Returns:
        str: Финальный ключ перемещённого файла.
    
    Raises:
        ClientError: Если исходный файл не найден или произошла ошибка копирования.
    """
    s3 = get_s3_client()
    
    # Применяем project_name если указан
    if project_name:
        source_filename = Path(source_key).name if os.path.sep in source_key or '/' in source_key else source_key
        dest_filename = Path(destination_key).name if os.path.sep in destination_key or '/' in destination_key else destination_key
        
        source_key = _build_key(project_name, source_filename)
        destination_key = _build_key(project_name, dest_filename)
    
    # Проверяем существование исходного файла
    if not file_exists(bucket, source_key):
        raise ClientError(
            error_response={'Error': {'Code': '404', 'Message': 'Source file not found'}},
            operation_name='HeadObject'
        )
    
    # Копируем файл в новое место
    copy_source = {'Bucket': bucket, 'Key': source_key}
    
    try:
        s3.copy_object(
            Bucket=bucket,
            CopySource=copy_source,
            Key=destination_key
        )
        print(f"File copied from {source_key} to {destination_key}")
        
        # Удаляем исходный файл если требуется
        if delete_source:
            s3.delete_object(Bucket=bucket, Key=source_key)
            print(f"Source file {source_key} deleted")
        
        return destination_key
        
    except ClientError as e: 
        raise ClientError(
            error_response={'Error': {'Code': e.response['Error']['Code'], 
                                     'Message': f"Failed to move file: {e.response['Error']['Message']}"}},
            operation_name='CopyObject'
        )


def move_file_with_rename(
    bucket: str,
    source_key: str,
    destination: str,
    new_filename: str,
    project_name: Optional[str] = None,
    delete_source: bool = True
) -> str:
    """
    Перемещает файл в указанную директорию с новым именем.
    
    Args:
        bucket (str): Название бакета.
        source_key (str): Исходный ключ файла.
        destination (str): Целевая директория (если заканчивается на '/') или полный путь.
        new_filename (str): Новое имя файла.
        project_name (Optional[str]): Если указан, применяется к путям.
        delete_source (bool): Удалять ли исходный файл.
    
    Returns:
        str: Финальный ключ перемещённого файла.
    
    Example:
        move_file_with_rename('bucket', 'old/file.txt', 'new/', 'renamed.txt')
        # Результат: old/file.txt → new/renamed.txt
        
        move_file_with_rename('bucket', 'file.txt', 'folder/subfolder', 'new.txt')
        # Результат: file.txt → folder/subfolder/new.txt
    """
    # Определяем, является ли destination директорией или полным путем
    if destination.endswith('/'):
        # destination - это директория
        dest_dir = destination.rstrip('/')
        final_destination = f"{dest_dir}/{new_filename}" if dest_dir else new_filename
    else:
        # Проверяем, содержит ли destination расширение файла
        # Если нет точки в последней части или есть слеш в конце - считаем директорией
        last_part = destination.split('/')[-1] if '/' in destination else destination
        if '.' not in last_part:
            # Вероятно директория
            final_destination = f"{destination.rstrip('/')}/{new_filename}" if destination else new_filename
        else:
            # destination уже содержит имя файла - заменяем его
            dest_dir = os.path.dirname(destination)
            if dest_dir:
                final_destination = f"{dest_dir}/{new_filename}"
            else:
                final_destination = new_filename
    
    # Используем основную функцию move_file
    return move_file(
        bucket=bucket,
        source_key=source_key,
        destination_key=final_destination,
        project_name=project_name,
        delete_source=delete_source
    )
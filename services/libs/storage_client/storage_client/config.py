from os import getenv
from dotenv import load_dotenv
from typing import Optional

load_dotenv()  # если есть .env в корне проекта

class Settings:
    MINIO_ENDPOINT: str = getenv("MINIO_ENDPOINT", "http://localhost:9000")
    MINIO_ACCESS_KEY: str = getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY: str = getenv("MINIO_SECRET_KEY", "minioadmin")
    MINIO_SECURE: bool = getenv("MINIO_SECURE", "false").lower() in ("1","true","yes")
    DEFAULT_BUCKET: Optional[str] = getenv("MINIO_BUCKET", "mapreduce-data")

settings = Settings()

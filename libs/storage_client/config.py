from os import getenv
from dotenv import load_dotenv
from typing import Optional

load_dotenv()

class Settings:
    MINIO_ENDPOINT: str = getenv("MINIO_ENDPOINT", "http://localhost:9000") # "172.21.53.148:9000"
    MINIO_ACCESS_KEY: str = getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY: str = getenv("MINIO_SECRET_KEY", "minioadmin")
    MINIO_SECURE: bool = False

settings = Settings()
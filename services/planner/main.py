import pika
import json
import uuid
import time
import os

from libs.storage_client.storage_client import upload_file


QUEUE_NAME = 'tasks'

RABBIT_PASS = 'password'
RABBIT_LOGIN = 'admin'
RABBIT_HOST = 'localhost'
RABBIT_PORT = 5672


def split_and_upload_txt(input_file: str, lines_per_file: int = 1_000_000, bucket="mapreduce", prefix="chunks/") -> list:
    '''Splits a large txt file into smaller parts and uploads them to MinIO.
    Returns a list of uploaded file names.
    '''

    with open(input_file, 'r', encoding='utf-8') as f:
        file_count = 0
        lines = []
        file_names = []
        for line in f:
            lines.append(line)
            if len(lines) >= lines_per_file:
                part_file = f"{input_file}_part{file_count}.txt"
                with open(part_file, 'w', encoding='utf-8') as pf:
                    pf.writelines(lines)
                key = f"{prefix}{os.path.basename(part_file)}"
                
                # Upload part file to MinIO
                upload_file(part_file, bucket, key)
                file_names.append(key)

                os.remove(part_file)
                file_count += 1
                lines = []
        if lines:
            part_file = f"{input_file}_part{file_count}.txt"
            with open(part_file, 'w', encoding='utf-8') as pf:
                pf.writelines(lines)
            key = f"{prefix}{os.path.basename(part_file)}"

            upload_file(part_file, bucket, key)
            file_names.append(key)

            os.remove(part_file)

        return file_names


def send_task(ch, task_type: str, address: str, main_task_id, task_id, storage: str = "minio", bucket: str = "mapreduce"):
    task = {
        "main_task_id": main_task_id,
        "task_id": task_id,
        "type": task_type,
        "address": address,   # s3 key, например "chunks/large_test_words.txt_part0.txt"
        "storage": storage,   # "minio" или "local"
        "bucket": bucket,
        "created_at": time.time()
    }
    body = json.dumps(task)
    props = pika.BasicProperties(delivery_mode=2, content_type='application/json')
    ch.basic_publish(exchange='', routing_key=QUEUE_NAME, body=body, properties=props)
    print(f"[Planner] sent task {task['task_id']} type={task_type} address={address} storage={storage}")



def main():
    credentials = pika.PlainCredentials(RABBIT_LOGIN, RABBIT_PASS)
    params = pika.ConnectionParameters(host=RABBIT_HOST, port=RABBIT_PORT, virtual_host='/', credentials=credentials)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE_NAME, durable=True)

    MAIN_TASK_ID = '1'
    INPUT_FILE = r"large_test_words.txt"
    BUCKET_NAME = "mapreduce"

    files = split_and_upload_txt(INPUT_FILE, lines_per_file=1_000_000, bucket=BUCKET_NAME, prefix=f"{MAIN_TASK_ID}/")
    print(files)

    for file_key in files:
        task_id = str(uuid.uuid4())
        send_task(ch, "map", address=file_key, main_task_id=MAIN_TASK_ID, task_id=task_id, storage="minio", bucket=BUCKET_NAME)

    for part_num in range(4):  # предполагаем 4 части для редьюса
        send_task(ch, "reduce", address=str(part_num), main_task_id=MAIN_TASK_ID, task_id=str(uuid.uuid4()), storage="minio", bucket=BUCKET_NAME)

    conn.close()


if __name__ == "__main__":
    main()

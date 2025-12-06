import pika
import json
import uuid
import time
import os

from storage_client import upload_file


QUEUE_NAME = 'tasks'

RABBIT_PASS = 'password'
RABBIT_LOGIN = 'admin'
RABBIT_HOST = 'localhost'
RABBIT_PORT = 5672

TASK_ID = str(uuid.uuid4())


def split_and_upload_txt(input_file: str, lines_per_file: int = 1_000_000, bucket="mapreduce", prefix="chunks/"):
    '''Splits a large txt file into smaller parts and uploads them to MinIO.'''

    with open(input_file, 'r', encoding='utf-8') as f:
        file_count = 0
        lines = []
        for line in f:
            lines.append(line)
            if len(lines) >= lines_per_file:
                part_file = f"{input_file}_part{file_count}.txt"
                with open(part_file, 'w', encoding='utf-8') as pf:
                    pf.writelines(lines)
                key = f"{prefix}{os.path.basename(part_file)}"
                
                # Upload part file to MinIO
                upload_file(part_file, bucket, key)

                os.remove(part_file)
                file_count += 1
                lines = []
        if lines:
            part_file = f"{input_file}_part{file_count}.txt"
            with open(part_file, 'w', encoding='utf-8') as pf:
                pf.writelines(lines)
            key = f"{prefix}{os.path.basename(part_file)}"

            upload_file(part_file, bucket, key)

            os.remove(part_file)


def send_map_task(ch, task_type: str, address: str, storage: str = "minio", bucket: str = "mapreduce"):
    task = {
        "task_id": TASK_ID,
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



def send_reduce_task(ch, task_type: str, part_id: int):

    task = {
        "task_id": TASK_ID,
        "type": task_type,
        "part_id": part_id,
        "created_at": time.time()
    }
    body = json.dumps(task)

    # delivery_mode=2 — сделать сообщение persistent
    props = pika.BasicProperties(delivery_mode=2, content_type='application/json')
    ch.basic_publish(exchange='', routing_key=QUEUE_NAME, body=body, properties=props)

    print(f"[Planner] sent task {task['task_id']} type={task_type} part_id={part_id}")


def main():
    credentials = pika.PlainCredentials(RABBIT_LOGIN, RABBIT_PASS)
    params = pika.ConnectionParameters(host=RABBIT_HOST, port=RABBIT_PORT, virtual_host='/', credentials=credentials)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE_NAME, durable=True)

    send_map_task(ch, "map", address="chunks/large_test_words.txt_part1.txt", storage="minio", bucket="mapreduce")

    # send_reduce_task(ch, "reduce", part_id=0)

    conn.close()


if __name__ == "__main__":
    # split_and_upload_txt(r"C:\ovr_pr\large_test_words.txt", lines_per_file=1_000_000, bucket="mapreduce", prefix="chunks/")
    main()

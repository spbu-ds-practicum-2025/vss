from libs.worker.worker import WordCountMapper, MapExecutor, WordCountShuffler, ShuffleExecutor, WordCountReducer, ReduceExecutor
from libs.worker.loaders import txtDataSource, jsonDataSink, jsonDataSource

from libs.storage_client.client import download_file, upload_file, list_objects

import shutil
import os
import pika
import json
import uuid
import threading
import time

QUEUE_NAME = 'tasks'
MAP_DONE_QUEUE = "map.done"

WORKER_ID = str(uuid.uuid4())[:8]

RABBIT_PASS = 'password'
RABBIT_LOGIN = 'admin'
RABBIT_HOST = 'localhost'
RABBIT_PORT = 7672

# number of retries for failed tasks
MAX_RETRIES = 3

def heartbeat_loop():
    credentials = pika.PlainCredentials(RABBIT_LOGIN, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PORT,
        virtual_host='/',
        credentials=credentials
    )

    while True:
        try:
            conn = pika.BlockingConnection(params)
            ch = conn.channel()

            # Get current task from the thread (if any)
            current_task = getattr(threading.current_thread(), "current_task", None)

            msg = {
                "event": "heartbeat",
                "worker_id": WORKER_ID,
                "ts": time.time()
            }

            if current_task:
                msg["current_task"] = {
                    "task_id": current_task.get("task_id"),
                    "main_task_id": current_task.get("main_task_id"),
                    "type": current_task.get("type"),
                    "address": current_task.get("address")
                }

            ch.basic_publish(
                exchange='',
                routing_key='events.worker',
                body=json.dumps(msg),
                properties=pika.BasicProperties(delivery_mode=2)
            )

            ch.close()
            conn.close()
        except Exception as e:
            print(f"[{WORKER_ID}] Heartbeat failed: {e}. Retrying in 10s...")

        time.sleep(10)

def download_part_files(main_task_id: str, part_num: int, bucket: str = "mapreduce") -> str:
    prefix = f"{main_task_id}/parts/part_{part_num}/"

    local_dir = os.path.join("storage", main_task_id, "parts", f"part_{part_num}")
    os.makedirs(local_dir, exist_ok=True)

    objects = list_objects(bucket, prefix)

    # ИСПРАВЛЕНИЕ: если нет файлов — просто возвращаем пустую директорию
    if not objects:
        print(f"[{WORKER_ID}] No files for part_{part_num} — empty partition, skipping download")
        return local_dir  # всё равно возвращаем директорию, reduce обработает пустую

    for obj_key in objects:
        local_path = os.path.join(local_dir, os.path.basename(obj_key))
        download_file(bucket=bucket, key=obj_key, local_path=local_path, project_name=None)

    return local_dir


def download_input_file(task: dict) -> str:
    """
    Ensure local directory exists, download from MinIO if requested and return local path.
    """
    task_id = task.get('task_id') or 'unknown'
    main_task_id = task.get('main_task_id') or 'unknown'
    local_dir = os.path.join("storage", main_task_id, task_id)
    local_path = os.path.join(local_dir, "input_file.txt")

    os.makedirs(local_dir, exist_ok=True)

    storage_type = task.get('storage')
    if storage_type is None:
        raise ValueError("Task missing 'storage' field (expected 'minio' or 'local').")

    if storage_type == 'minio':
        s3_key = task.get('address')
        if not s3_key:
            raise ValueError("Task missing 'address' (S3 object key).")
        bucket = task.get('bucket', "mapreduce")
        # Добавлен project_name=None
        download_file(
            bucket=bucket,
            key=s3_key,
            local_path=local_path,
            project_name=None
        )
        return local_path

    elif storage_type == 'local':
        addr = task.get('address')
        if not addr:
            raise ValueError("Task missing 'address' for local storage.")
        if not os.path.exists(addr):
            raise FileNotFoundError(f"Local input file not found: {addr}")
        return addr

    else:
        raise ValueError(f"Unknown storage type: {storage_type}")


def callback(ch, method, properties, body):
    try:
        task = json.loads(body)
    except Exception:
        print(f"[{WORKER_ID}] invalid message, ack and skip")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    print(f"[{WORKER_ID}] picked {task.get('task_id')}")

    threading.current_thread().current_task = task

    ch.basic_publish(
        exchange='',
        routing_key='events.worker',
        body=json.dumps({
            "event": "task.started",
            "worker_id": WORKER_ID,
            "task": task
        }),
        properties=pika.BasicProperties(delivery_mode=2)
    )

    SPILL_FILES_DIR = rf"storage\{task.get('main_task_id')}\{task.get('task_id')}\spill_files"
    SHUFFLE_FILES_DIR = rf"storage\{task.get('main_task_id')}\{task.get('task_id')}\shuffle_files"
    REDUCE_OUTPUT_DIR = rf"storage\{task.get('main_task_id')}\{task.get('task_id')}\reduce_output"

    def process_map_task(file_address: str):
        time.sleep(10)
        mapper = WordCountMapper()
        data_spill_saver = jsonDataSink(SPILL_FILES_DIR, mode="jsonl")
        data_sorce = txtDataSource()
        
        map_executor = MapExecutor(mapper, data_spill_saver, data_sorce, threshold=5_000)
        map_executor.process(filepath=file_address)

        print("Mapping phase completed. Starting shuffling phase...")

        shuffler = WordCountShuffler(num_parts=4, flush_threshold=2_000)
        shuffle_executor = ShuffleExecutor(shuffler, 
                                        source=jsonDataSource(),
                                            sink=jsonDataSink(SHUFFLE_FILES_DIR, mode="jsonl"))
        shuffle_executor.process(SPILL_FILES_DIR)
        print("Shuffling phase completed.")
        shutil.rmtree(SPILL_FILES_DIR)


    def process_reduce_task(part_num: int):
        print("Starting reducing phase...")
        reducer = WordCountReducer()
        part_dir = download_part_files(task.get('main_task_id'), part_num, bucket=task.get('bucket', 'mapreduce'))
        print(f"Downloaded part files to {part_dir}")

        reduce_executor = ReduceExecutor(reducer,
                                        sink=jsonDataSink(REDUCE_OUTPUT_DIR, mode="jsonl"),
                                        source=jsonDataSource())
        
        reduce_executor.process(part_dir=part_dir, part_num=part_num)

        print("Reducing phase completed.")
        print(f"Reduce output stored in: {REDUCE_OUTPUT_DIR}")

    headers = {}
    if properties is not None:
        headers = properties.headers or {}
    attempts = int(headers.get('x-attempts', 0))

    main_task_id = task.get('main_task_id')
    worker_task_id = task.get('task_id')

    try:
        if task.get('type') == 'map':
            input_file = download_input_file(task)
            process_map_task(input_file)
            ch.basic_ack(delivery_tag=method.delivery_tag) 

            if not main_task_id:
                raise ValueError("Missing main_task_id in task")

            local_shuffle_dir = SHUFFLE_FILES_DIR

            if not os.path.isdir(local_shuffle_dir):
                print(f"[{WORKER_ID}] no shuffle dir at {local_shuffle_dir}, nothing to upload")
            else:
                print(f"[{WORKER_ID}] uploading shuffle files from {local_shuffle_dir} to bucket 'mapreduce'")

                upload_failures = []
                uploaded = 0

                import re
                for filename in sorted(os.listdir(local_shuffle_dir)):
                    local_path = os.path.join(local_shuffle_dir, filename)
                    if not os.path.isfile(local_path):
                        continue

                    m = re.search(r'part[_\-]?(\d+)', filename, flags=re.IGNORECASE)
                    if m:
                        part_idx = int(m.group(1))
                    else:
                        m2 = re.match(r'^(\d+)[_\.\-]', filename)
                        part_idx = int(m2.group(1)) if m2 else "unknown"

                    s3_key = f"{main_task_id}/parts/part_{part_idx}/{worker_task_id}_{filename}"
                    try:
                        # ИСПРАВЛЕННЫЙ ВЫЗОВ
                        upload_file(
                            local_path=local_path,
                            bucket="mapreduce",
                            project_name=None,  # важно!
                            key=s3_key
                        )
                        uploaded += 1
                        print(f"[{WORKER_ID}] uploaded {filename} -> {s3_key}")
                    except Exception as e:
                        upload_failures.append((filename, str(e)))
                        print(f"[{WORKER_ID}] ERROR uploading {filename}: {e}")

                if upload_failures:
                    print(f"[{WORKER_ID}] upload finished with {len(upload_failures)} failures: {upload_failures}")
                    raise RuntimeError(f"Upload errors: {len(upload_failures)} files failed")
                else:
                    print(f"[{WORKER_ID}] Upload completed. {uploaded} files uploaded.")

                    local_task_dir = os.path.join("storage", main_task_id, worker_task_id)
                    if os.path.isdir(local_task_dir):
                        try:
                            shutil.rmtree(local_task_dir)
                            print(f"[{WORKER_ID}] cleaned up local files: {local_task_dir}")
                        except Exception as e:
                            print(f"[{WORKER_ID}] warning: failed to remove {local_task_dir}: {e}")

            ch.basic_publish(
                exchange='',
                routing_key='events.worker',
                body=json.dumps({
                    "event": "map.done",
                    "main_task_id": main_task_id,
                    "task_id": worker_task_id
                }),
                properties=pika.BasicProperties(delivery_mode=2)
            )

        elif task.get('type') == 'reduce':
            process_reduce_task(int(task.get('address')))
            ch.basic_ack(delivery_tag=method.delivery_tag)
            s3_key = f"{main_task_id}/reduce_output/reduced_part_{task.get('address')}.jsonl"
            local_reduce_file = os.path.join(REDUCE_OUTPUT_DIR, f"reduced_{task.get('address')}.jsonl")
            # ИСПРАВЛЕННЫЙ ВЫЗОВ
            upload_file(
                local_path=local_reduce_file,
                bucket="mapreduce",
                project_name=None,
                key=s3_key
            )

            print(f"[{WORKER_ID}] completed {task.get('task_id')} type=reduce")

            ch.basic_publish(
                exchange='',
                routing_key='events.worker',
                body=json.dumps({
                    "event": "reduce.done",
                    "main_task_id": main_task_id,
                    "part": task.get("address")
                }),
                properties=pika.BasicProperties(delivery_mode=2)
            )

    except Exception as e:
        print(f"[{WORKER_ID}] error processing {task.get('task_id')}: {e}")

        if attempts + 1 >= MAX_RETRIES:
            headers['x-attempts'] = attempts + 1
            ch.basic_publish(
                exchange='',
                routing_key='tasks.dead',
                body=body,
                properties=pika.BasicProperties(headers=headers, delivery_mode=2)
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[{WORKER_ID}] sent to dead queue: attempts={attempts+1}")
        else:
            headers['x-attempts'] = attempts + 1
            ch.basic_publish(
                exchange='',
                routing_key=QUEUE_NAME,
                body=body,
                properties=pika.BasicProperties(headers=headers, delivery_mode=2)
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[{WORKER_ID}] requeued task (attempt {attempts+1})")

    finally:
        if hasattr(threading.current_thread(), "current_task"):
            delattr(threading.current_thread(), "current_task")

        ch.basic_publish(
            exchange='',
            routing_key='events.worker',
            body=json.dumps({
                "event": "task.done",
                "worker_id": WORKER_ID,
                "task_id": task.get("task_id")
            }),
            properties=pika.BasicProperties(delivery_mode=2)
        )


def main():
    credentials = pika.PlainCredentials(RABBIT_LOGIN, RABBIT_PASS)
    params = pika.ConnectionParameters(host=RABBIT_HOST, port=RABBIT_PORT, virtual_host='/', credentials=credentials)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()

    ch.queue_declare(queue=QUEUE_NAME, durable=True)
    ch.basic_qos(prefetch_count=1)

    ch.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=False)

    print(f"[{WORKER_ID}] waiting for tasks. To exit press CTRL+C")
    try:
        heartbeat = threading.Thread(target=heartbeat_loop, daemon=True)
        heartbeat.start()

        ch.start_consuming()
    except KeyboardInterrupt:
        ch.stop_consuming()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
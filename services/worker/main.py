from worker import WordCountMapper, MapExecutor, WordCountShuffler, ShuffleExecutor, WordCountReducer, ReduceExecutor, DataManager
from loaders import txtDataSource, jsonDataSink, txtDataSink, jsonDataSource

from storage_client import download_file, upload_file

import shutil
import os
import pika
import json
import uuid

QUEUE_NAME = 'tasks'
WORKER_ID = "1"

RABBIT_PASS = 'password'
RABBIT_LOGIN = 'admin'
RABBIT_HOST = 'localhost'
RABBIT_PORT = 5672

# numsber of retries for failed tasks
MAX_RETRIES = 3


def download_input_file(task: dict) -> str:
    """
    Ensure local directory exists, download from MinIO if requested and return local path.
    Raises informative exceptions on bad input.
    """
    task_id = task.get('task_id') or 'unknown'
    local_dir = os.path.join("storage", task_id)
    local_path = os.path.join(local_dir, "input_file.txt")

    # ensure storage dir exists
    os.makedirs(local_dir, exist_ok=True)

    storage_type = task.get('storage')  # <- note: правильное поле 'storage'
    if storage_type is None:
        raise ValueError("Task missing 'storage' field (expected 'minio' or 'local').")

    if storage_type == 'minio':
        # require address (S3 key) and optionally bucket
        s3_key = task.get('address')
        if not s3_key:
            raise ValueError("Task missing 'address' (S3 object key).")
        bucket = task.get('bucket', "mapreduce")  # fallback default bucket
        # download into existing folder
        download_file(bucket=bucket, key=s3_key, local_path=local_path)
        # now return the local filesystem path for downstream processors
        return local_path

    elif storage_type == 'local':
        # address is a local path already: just return it (or copy if you need)
        addr = task.get('address')
        if not addr:
            raise ValueError("Task missing 'address' for local storage.")
        if not os.path.exists(addr):
            raise FileNotFoundError(f"Local input file not found: {addr}")
        return addr

    else:
        raise ValueError(f"Unknown storage type: {storage_type}")


def callback(ch, method, properties, body):
    # if message is not valid JSON — ack and skip
    try:
        task = json.loads(body)
    except Exception:
        print(f"[{WORKER_ID}] invalid message, ack and skip")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    print(f"[{WORKER_ID}] picked {task.get('task_id')}")

    SPILL_FILES_DIR = rf"storage\{task.get('task_id')}\spill_files"
    SHUFFLE_FILES_DIR = rf"storage\{task.get('task_id')}\shuffle_files"
    REDUCE_OUTPUT_DIR = rf"storage\{task.get('task_id')}\reduce_output"
            

    def process_map_task(file_address: str):

        # mapping phase
        mapper = WordCountMapper()
        data_spill_saver = jsonDataSink(SPILL_FILES_DIR, mode="jsonl")
        data_sorce = txtDataSource()
        
        map_executor = MapExecutor(mapper, data_spill_saver, data_sorce, threshold=5_000)
        map_executor.process(filepath=file_address)

        print("Mapping phase completed. Starting shuffling phase...")

        # shuffling phase

        shuffler = WordCountShuffler(num_parts=4, flush_threshold=2_000)
        shuffle_executor = ShuffleExecutor(shuffler, 
                                        source=jsonDataSource(),
                                            sink=jsonDataSink(SHUFFLE_FILES_DIR, mode="jsonl"))
        shuffle_executor.process(SPILL_FILES_DIR)
        print("Shuffling phase completed.")
        shutil.rmtree(SPILL_FILES_DIR)


    def process_reduce_task(file_address: str, part_num: int):
        # reducing phase

        if not os.path.exists(SHUFFLE_FILES_DIR):
            raise FileNotFoundError(f"Spill files dir not found: {SHUFFLE_FILES_DIR}")

        reducer = WordCountReducer()
        reduce_executor = ReduceExecutor(reducer,
                                        sink=jsonDataSink(REDUCE_OUTPUT_DIR, mode="jsonl"),
                                        source=jsonDataSource())
        
        reduce_executor.process(part_dir=SHUFFLE_FILES_DIR, part_num=part_num)

        print("Reducing phase completed.")



    headers = {}
    if properties is not None:
        # properties.headers может быть None или dict
        headers = properties.headers or {}
    attempts = int(headers.get('x-attempts', 0))


    try:
        input_file = download_input_file(task)

        if task.get('type') == 'map':
            process_map_task(input_file)
            ch.basic_ack(delivery_tag=method.delivery_tag) 
            print(f"[{WORKER_ID}] completed {task.get('task_id')} type=map")
            
            print(f"trying to upload shuffle files from {SHUFFLE_FILES_DIR}")
            for filename in os.listdir(SHUFFLE_FILES_DIR):
                upload_file(os.path.join(SHUFFLE_FILES_DIR, filename), 
                            bucket="mapreduce",
                            key=f"shuffle/{task.get('task_id')}/{filename}")
                
            print("Upload of shuffle files completed.")

            print(f"Cleaning up local files for task {task.get('task_id')}")
            shutil.rmtree(os.path.join("storage", task.get('task_id'))) 
            print("Cleanup completed.")
 


        elif task.get('type') == 'reduce':
            process_reduce_task(input_file, task.get('part_id'))
            ch.basic_ack(delivery_tag=method.delivery_tag) 
            print(f"[{WORKER_ID}] completed {task.get('task_id')} type=reduce part_id={task.get('part_id')}")

    except Exception as e:
        print(f"[{WORKER_ID}] error processing {task.get('task_id')}: {e}")

        # retry logic
        if attempts + 1 >= MAX_RETRIES:
            # отправляем в dead-letter, передав attempts для истории
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
            # републикуем задачу с увеличенным counter (и ack текущую)
            headers['x-attempts'] = attempts + 1
            ch.basic_publish(
                exchange='',
                routing_key=QUEUE_NAME,
                body=body,
                properties=pika.BasicProperties(headers=headers, delivery_mode=2)
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[{WORKER_ID}] requeued task (attempt {attempts+1})")


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
        ch.start_consuming()
    except KeyboardInterrupt:
        ch.stop_consuming()
    finally:
        conn.close()


if __name__ == "__main__":
    main()



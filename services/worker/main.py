from worker import WordCountMapper, MapExecutor, WordCountShuffler, ShuffleExecutor, WordCountReducer, ReduceExecutor
from loaders import txtDataSource, jsonDataSink, jsonDataSource

from storage_client.client import download_file, upload_file, list_objects


import shutil
import os
import pika
import json

QUEUE_NAME = 'tasks'
WORKER_ID = "1"

RABBIT_PASS = 'password'
RABBIT_LOGIN = 'admin'
RABBIT_HOST = 'localhost'
RABBIT_PORT = 5672

# numsber of retries for failed tasks
MAX_RETRIES = 3

def download_part_files(main_task_id: str, part_num: int, bucket: str = "mapreduce") -> str:
    """
    Download all shuffle part files for a specific part number from S3 into local storage.

    Args:
        main_task_id (str): ID of the whole MapReduce job.
        part_num (int): Partition number (the "N" in part_N).
        bucket (str): S3 bucket name.

    Returns:
        str: Path to the local directory containing downloaded part files.
    """

    # S3 prefix: MAIN_TASK_ID/shuffle/part_{n}/
    prefix = f"{main_task_id}/parts/part_{part_num}/"

    # Local directory where files will be stored
    local_dir = os.path.join("storage", main_task_id, "parts", f"part_{part_num}")
    os.makedirs(local_dir, exist_ok=True)

    # List all objects in this prefix
    objects = list_objects(bucket, prefix)
    if not objects:
        raise FileNotFoundError(f"No files found in S3 path: {prefix}")

    # Download all of them
    for obj_key in objects:
        local_path = os.path.join(local_dir, os.path.basename(obj_key))
        download_file(bucket, obj_key, local_path)

    return local_dir


def download_input_file(task: dict) -> str:
    """
    Ensure local directory exists, download from MinIO if requested and return local path.
    Raises informative exceptions on bad input.
    """
    task_id = task.get('task_id') or 'unknown'
    main_task_id = task.get('main_task_id') or 'unknown'
    local_dir = os.path.join("storage", main_task_id, task_id)
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

    SPILL_FILES_DIR = rf"storage\{task.get('main_task_id')}\{task.get('task_id')}\spill_files"
    SHUFFLE_FILES_DIR = rf"storage\{task.get('main_task_id')}\{task.get('task_id')}\shuffle_files"
    REDUCE_OUTPUT_DIR = rf"storage\{task.get('main_task_id')}\{task.get('task_id')}\reduce_output"
            

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


    def process_reduce_task(part_num: int):
        # reducing phase
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
        # properties.headers может быть None или dict
        headers = properties.headers or {}
    attempts = int(headers.get('x-attempts', 0))

    main_task_id = task.get('main_task_id')
    worker_task_id = task.get('task_id')


    try:

        if task.get('type') == 'map':
            input_file = download_input_file(task)
            process_map_task(input_file)
            ch.basic_ack(delivery_tag=method.delivery_tag) 
            # --- upload shuffle files to S3 (simple, robust) ---
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

                    # try to detect part index from filename, fallback to 'unknown'
                    m = re.search(r'part[_\-]?(\d+)', filename, flags=re.IGNORECASE)
                    if m:
                        part_idx = int(m.group(1))
                    else:
                        # try a leading number like "0_" or "0."
                        m2 = re.match(r'^(\d+)[_\.\-]', filename)
                        part_idx = int(m2.group(1)) if m2 else "unknown"

                    s3_key = f"{main_task_id}/parts/part_{part_idx}/{worker_task_id}_{filename}"
                    try:
                        # upload_file(local_path, bucket, key)
                        upload_file(local_path, bucket="mapreduce", key=s3_key)
                        uploaded += 1
                        print(f"[{WORKER_ID}] uploaded {filename} -> {s3_key}")
                    except Exception as e:
                        upload_failures.append((filename, str(e)))
                        print(f"[{WORKER_ID}] ERROR uploading {filename}: {e}")

                if upload_failures:
                    # don't remove anything if uploads weren't all successful
                    print(f"[{WORKER_ID}] upload finished with {len(upload_failures)} failures: {upload_failures}")
                    raise RuntimeError(f"Upload errors: {len(upload_failures)} files failed")
                else:
                    print(f"[{WORKER_ID}] Upload completed. {uploaded} files uploaded.")

                    # cleanup local task folder (safe remove)
                    local_task_dir = os.path.join("storage", main_task_id, worker_task_id)
                    if os.path.isdir(local_task_dir):
                        try:
                            shutil.rmtree(local_task_dir)
                            print(f"[{WORKER_ID}] cleaned up local files: {local_task_dir}")
                        except Exception as e:
                            print(f"[{WORKER_ID}] warning: failed to remove {local_task_dir}: {e}")
                    else:
                        print(f"[{WORKER_ID}] nothing to cleanup at {local_task_dir}")


        elif task.get('type') == 'reduce':
            process_reduce_task(int(task.get('address')))
            ch.basic_ack(delivery_tag=method.delivery_tag)
            s3_key = f"{main_task_id}/reduce_output/reduced_part_{task.get('address')}.jsonl"
            local_reduce_file = os.path.join(REDUCE_OUTPUT_DIR, f"reduced_{task.get('address')}.jsonl")
            upload_file(local_reduce_file, bucket="mapreduce", key=s3_key)

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



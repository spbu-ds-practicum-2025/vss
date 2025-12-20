import pika
import json
import uuid
import os
import shutil

from libs.storage_client.client import upload_file, download_file, list_objects
from libs.worker.loaders import jsonDataSource, jsonDataSink
from libs.worker.loaders import DataSource, DataSink

RABBIT_PASS = 'password'
RABBIT_LOGIN = 'admin'
RABBIT_HOST = 'localhost'
RABBIT_PORT = 7672

DATA_MANAGER_QUEUE = 'data_manager.requests'
DATA_MANAGER_RESPONSE_QUEUE = 'data_manager.responses'
BUCKET_NAME = "mapreduce"


class DataManager:

    @classmethod
    def split_and_upload_txt(
        cls,
        input_key: str,
        bucket: str,
        prefix: str = "",
        lines_per_file: int = 1_000_000
    ) -> list:
        local_input = f"temp_input_{uuid.uuid4().hex}.txt"
        uploaded = []

        try:
            print(f"[DataManager] Downloading {input_key} from bucket {bucket}")
            download_file(bucket=bucket, key=input_key, local_path=local_input, project_name=None)

            with open(local_input, 'r', encoding='utf-8') as f:
                part_idx = 0
                buffer = []

                for line in f:
                    buffer.append(line)
                    if len(buffer) >= lines_per_file:
                        part_name = f"chunk_part{part_idx}.txt"
                        local_part = f"temp_part_{part_idx}_{uuid.uuid4().hex}.txt"

                        with open(local_part, 'w', encoding='utf-8') as pf:
                            pf.writelines(buffer)

                        key = f"{prefix}{part_name}"
                        upload_file(local_part, bucket, None, key)
                        uploaded.append(key)

                        os.remove(local_part)
                        buffer.clear()
                        part_idx += 1

                if buffer:
                    part_name = f"chunk_part{part_idx}.txt"
                    local_part = f"temp_part_{part_idx}_{uuid.uuid4().hex}.txt"
                    with open(local_part, 'w', encoding='utf-8') as pf:
                        pf.writelines(buffer)
                    key = f"{prefix}{part_name}"
                    upload_file(local_part, bucket, None, key)
                    uploaded.append(key)
                    os.remove(local_part)

            print(f"[DataManager] Split completed: {len(uploaded)} chunks uploaded")
            return uploaded

        finally:
            if os.path.exists(local_input):
                os.remove(local_input)

    @classmethod
    def manage_reduce_data(cls, source: DataSource, sink: DataSink, dirpath: str) -> dict:
        combined_data = {}
        for file in os.listdir(dirpath):
            filepath = os.path.join(dirpath, file)
            for record in source.load(filepath):
                combined_data.update(record)

        combined_data = dict(sorted(combined_data.items(), key=lambda item: item[0]))
        sink.save(combined_data, name='result')
        return combined_data

    @classmethod
    def merge_reduce_files(cls, main_task_id: str, bucket: str) -> str:
        reduce_prefix = f"{main_task_id}/reduce_output/"
        local_dir = f"temp_reduce_{uuid.uuid4().hex}"
        os.makedirs(local_dir, exist_ok=True)

        try:
            objects = list_objects(bucket, reduce_prefix)
            reduce_files = [k for k in objects if k.endswith('.jsonl')]

            if not reduce_files:
                raise ValueError("No reduce files found")

            for obj_key in reduce_files:
                local_path = os.path.join(local_dir, os.path.basename(obj_key))
                download_file(bucket=bucket, key=obj_key, local_path=local_path, project_name=None)

            source = jsonDataSource()
            sink = jsonDataSink(local_dir, mode="jsonl")
            cls.manage_reduce_data(source, sink, local_dir)  # ← теперь работает!

            result_local = os.path.join(local_dir, "result.jsonl")
            result_key = f"{main_task_id}/output/result.jsonl"
            upload_file(result_local, bucket, None, result_key)

            print(f"[DataManager] Merged reduce files → {result_key}")
            return result_key

        finally:
            if os.path.exists(local_dir):
                shutil.rmtree(local_dir)

def callback(ch, method, properties, body):
    msg = json.loads(body)

    action = msg.get("action")

    if action == "split_txt":
        input_key = msg["input_key"]
        bucket = msg.get("bucket", BUCKET_NAME)
        prefix = msg.get("prefix", "")
        main_task_id = msg["main_task_id"]
        correlation_id = properties.correlation_id

        print(f"[DataManager] Processing split request for {input_key} (task {main_task_id})")

        chunk_keys = DataManager.split_and_upload_txt(
            input_key=input_key,
            bucket=bucket,
            prefix=prefix,
            lines_per_file=msg.get("lines_per_file", 1_000_000)
        )

        response = {
            "main_task_id": main_task_id,
            "chunk_keys": chunk_keys,
            "status": "success" if chunk_keys else "failed"
        }

        ch.basic_publish(
            exchange='',
            routing_key=properties.reply_to,
            properties=pika.BasicProperties(correlation_id=correlation_id),
            body=json.dumps(response)
        )

    elif action == "merge_reduce":
        main_task_id = msg["main_task_id"]
        bucket = msg.get("bucket", BUCKET_NAME)
        correlation_id = properties.correlation_id

        print(f"[DataManager] Merging reduce files for task {main_task_id}")

        try:
            result_key = DataManager.merge_reduce_files(main_task_id, bucket)

            response = {
                "main_task_id": main_task_id,
                "result_key": result_key,
                "status": "success"
            }
        except Exception as e:
            print(f"[DataManager] Merge failed: {e}")
            response = {
                "main_task_id": main_task_id,
                "status": "failed",
                "error": str(e)
            }

        ch.basic_publish(
            exchange='',
            routing_key=properties.reply_to,
            properties=pika.BasicProperties(correlation_id=correlation_id),
            body=json.dumps(response)
        )

    else:
        print(f"[DataManager] Unknown action: {action}")

    ch.basic_ack(method.delivery_tag)


def start_data_manager():
    credentials = pika.PlainCredentials(RABBIT_LOGIN, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PORT,
        virtual_host='/',
        credentials=credentials
    )

    conn = pika.BlockingConnection(params)
    ch = conn.channel()

    ch.queue_declare(queue=DATA_MANAGER_QUEUE, durable=True)
    ch.queue_declare(queue=DATA_MANAGER_RESPONSE_QUEUE, durable=True)

    ch.basic_consume(queue=DATA_MANAGER_QUEUE, on_message_callback=callback, auto_ack=False)

    print("[DataManager] Waiting for requests (split_txt or merge_reduce)...")
    ch.start_consuming()


if __name__ == "__main__":
    start_data_manager()
import pika
import json
import uuid
import time
import os
import queue

from libs.storage_client.client import upload_file, upload_process_file, download_file




RABBIT_PASS = 'password'
RABBIT_LOGIN = 'admin'
RABBIT_HOST = 'localhost'
RABBIT_PORT = 7672

TASKS_QUEUE = 'tasks'
WORKER_EVENTS_QUEUE = 'events.worker'        # consumed by Task Manager
PLANNER_EVENTS_QUEUE = 'events.planner'       # planner listens here (from Task Manager)
DATA_MANAGER_QUEUE = 'data_manager.requests'  # NEW: queue for requesting split from DataManager
DATA_MANAGER_RESPONSE_QUEUE = 'data_manager.responses'  # NEW: queue where DataManager sends back chunk keys
API_QUEUE = 'user_requests'

NUM_REDUCE_PARTS = 4
BUCKET_NAME = "mapreduce"

# In-memory state
main_tasks = {}
correlation_ids = {}  # correlation_id -> main_task_id (to match responses)

recieved_messages = queue.Queue()

def get_task(ch, method, properties, body):

    data = json.loads(body.decode('utf-8'))
    print(f"[PLANNER] : got task {data}")
    recieved_messages.put(data)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def send_task(ch, task_type: str, address: str, main_task_id, task_id, storage: str = "minio", bucket: str = "mapreduce"):
    task = {
        "main_task_id": main_task_id,
        "task_id": task_id,
        "type": task_type,
        "address": address,
        "storage": storage,
        "bucket": bucket,
        "created_at": time.time()
    }
    ch.basic_publish(
        exchange='',
        routing_key=TASKS_QUEUE,
        body=json.dumps(task),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type='application/json'
        )
    )
    print(f"[Planner] sent {task_type} task {task_id} address={address}")

def request_split_from_data_manager(ch, input_key: str, main_task_id: str, prefix: str):
    correlation_id = str(uuid.uuid4())
    correlation_ids[correlation_id] = main_task_id

    message = {
        "action": "split_txt",
        "input_key": input_key,
        "bucket": BUCKET_NAME,
        "prefix": prefix,
        "lines_per_file": 1_000_000,
        "main_task_id": main_task_id,
        "correlation_id": correlation_id
    }

    ch.basic_publish(
        exchange='',
        routing_key=DATA_MANAGER_QUEUE,
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,
            correlation_id=correlation_id,
            reply_to=DATA_MANAGER_RESPONSE_QUEUE
        )
    )
    print(f"[Planner] requested split for {input_key} (prefix={prefix}), corr_id={correlation_id}")


def data_manager_response_callback(ch, method, properties, body):
    msg = json.loads(body)
    correlation_id = properties.correlation_id

    if correlation_id not in correlation_ids:
        print(f"[Planner] Received response with unknown correlation_id {correlation_id}")
        ch.basic_ack(method.delivery_tag)
        return

    main_task_id = correlation_ids.pop(correlation_id)
    chunk_keys = msg.get("chunk_keys", [])

    if not chunk_keys:
        print(f"[Planner] Split failed for task {main_task_id}")
        ch.basic_ack(method.delivery_tag)
        return

    print(f"[Planner] Received {len(chunk_keys)} chunks for task {main_task_id}")

    # Initialize state
    main_tasks[main_task_id] = {
        "chunks": chunk_keys,
        "chunk_status": {k: 'pending' for k in chunk_keys},
        "task_map": {}
    }

    # Notify Task Manager about expected map tasks
    ch.basic_publish(
        exchange='',
        routing_key=WORKER_EVENTS_QUEUE,
        body=json.dumps({
            "event": "map.expected",
            "main_task_id": main_task_id,
            "count": len(chunk_keys)
        }),
        properties=pika.BasicProperties(delivery_mode=2)
    )

    # Send map tasks
    for key in chunk_keys:
        task_id = str(uuid.uuid4())
        main_tasks[main_task_id]['task_map'][task_id] = key
        send_task(
            ch,
            task_type="map",
            address=key,
            main_task_id=main_task_id,
            task_id=task_id,
            storage="minio",
            bucket=BUCKET_NAME
        )

    print("[Planner] map tasks sent")
    ch.basic_ack(method.delivery_tag)


# Existing planner event callback (unchanged except minor fixes)
def planner_event_callback(ch, method, properties, body):
    msg = json.loads(body)
    event = msg.get("event")

    if event == "worker.failed":
        worker_id = msg.get("worker_id")
        failed_task = msg.get("failed_task")
        print(f"[Planner] got worker.failed for worker={worker_id} — requeueing pending map tasks")
        total_requeued = 0
        for main_task_id, info in main_tasks.items():
            for chunk_key, status in list(info['chunk_status'].items()):
                if status != 'done':
                    new_task_id = str(uuid.uuid4())
                    send_task(
                        ch,
                        task_type="map",
                        address=chunk_key,
                        main_task_id=main_task_id,
                        task_id=new_task_id,
                        storage="minio",
                        bucket=BUCKET_NAME
                    )
                    info['task_map'][new_task_id] = chunk_key
                    total_requeued += 1

        if failed_task:
            # Точная переотправка только упавшей задачи
            new_task_id = str(uuid.uuid4())
            send_task(
                ch,
                task_type=failed_task.get("type", "map"),
                address=failed_task.get("address"),
                main_task_id=failed_task.get("main_task_id"),
                task_id=new_task_id
            )
            # Обновляем маппинг
            main_task_id = failed_task.get("main_task_id")
            if main_task_id in main_tasks:
                main_tasks[main_task_id]['task_map'][new_task_id] = failed_task.get("address")
                main_tasks[main_task_id]['chunk_status'][failed_task.get("address")] = 'pending'

            print(f"[Planner] Precisely requeued failed task {new_task_id} -> {failed_task.get('address')}")

        print(f"[Planner] requeued {total_requeued} tasks due to worker failure")
        ch.basic_ack(method.delivery_tag)
        return

    if event in ("map.task.done", "map.done"):
        main_task_id = msg.get("main_task_id")
        task_id = msg.get("task_id")
        if main_task_id in main_tasks:
            mapping = main_tasks[main_task_id]['task_map']
            chunk = mapping.get(task_id)
            if chunk:
                main_tasks[main_task_id]['chunk_status'][chunk] = 'done'
                print(f"[Planner] map task {task_id} done -> chunk {chunk} marked done")
            else:
                for tid, ck in list(mapping.items()):
                    if tid == task_id:
                        main_tasks[main_task_id]['chunk_status'][ck] = 'done'
                        break
        ch.basic_ack(method.delivery_tag)
        return

    if event == "map.phase.done":
        main_task_id = msg.get("main_task_id")
        print(f"[Planner] map phase completed for {main_task_id}")
        ch.basic_publish(
            exchange='',
            routing_key=WORKER_EVENTS_QUEUE,
            body=json.dumps({
                "event": "reduce.expected",
                "main_task_id": main_task_id,
                "count": NUM_REDUCE_PARTS
            }),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        for part in range(NUM_REDUCE_PARTS):
            send_task(
                ch,
                task_type="reduce",
                address=str(part),
                main_task_id=main_task_id,
                task_id=str(uuid.uuid4()),
                storage="minio",
                bucket=BUCKET_NAME
            )
        ch.basic_ack(method.delivery_tag)
        return

    if event == "reduce.phase.done":
        print(f"[Planner] JOB {msg['main_task_id']} COMPLETED")
        ch.basic_ack(method.delivery_tag)
        return

    print(f"[Planner] unknown planner event: {event}")
    ch.basic_ack(method.delivery_tag)


def start_listeners(channel):
    channel.basic_consume(queue=PLANNER_EVENTS_QUEUE, on_message_callback=planner_event_callback, auto_ack=False)
    channel.basic_consume(queue=DATA_MANAGER_RESPONSE_QUEUE, on_message_callback=data_manager_response_callback, auto_ack=False)
    print("[Planner] Waiting for events and DataManager responses...")
    channel.start_consuming()



def main():
    print(f"[PLANNER] started")
    credentials = pika.PlainCredentials(RABBIT_LOGIN, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PORT,
        virtual_host='/',
        credentials=credentials
    )
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    # Declare all queues
    ch.queue_declare(queue=TASKS_QUEUE, durable=True)
    ch.queue_declare(queue=WORKER_EVENTS_QUEUE, durable=True)
    ch.queue_declare(queue=PLANNER_EVENTS_QUEUE, durable=True)
    ch.queue_declare(queue=DATA_MANAGER_QUEUE, durable=True)
    ch.queue_declare(queue=DATA_MANAGER_RESPONSE_QUEUE, durable=True)
    ch.queue_declare(queue=API_QUEUE, durable=True)

    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=API_QUEUE, on_message_callback=get_task, auto_ack=False)


    # MAIN_TASK_ID = str(uuid.uuid4())[:8]
    # INPUT_FILE_LOCAL = r"C:\ovr_pr\large_test_words.txt"
    # INPUT_KEY = f"{MAIN_TASK_ID}/input.txt"  # key in MinIO

    # print("[Planner] Uploading full input file to MinIO...")
    # upload_file(INPUT_FILE_LOCAL, BUCKET_NAME, INPUT_KEY)
    # print(f"[Planner] Uploaded input file as {INPUT_KEY}")

    # # Request split from DataManager
    # request_split_from_data_manager(
    #     ch,
    #     input_key=INPUT_KEY,
    #     main_task_id=MAIN_TASK_ID,
    #     prefix=f"{MAIN_TASK_ID}/"
    # )
    start_listeners(ch)

    while True:
        MAIN_TASK_ID = recieved_messages.get()
        INPUT_FILE = f"{MAIN_TASK_ID}/input_file/process.txt"
        BUCKET_NAME = "mapreduce"






if __name__ == "__main__":
    main()

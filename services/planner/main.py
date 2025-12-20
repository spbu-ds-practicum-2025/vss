import pika
import json
import uuid
import time
import os

from libs.storage_client.client import upload_file, download_file

RABBIT_PASS = 'password'
RABBIT_LOGIN = 'admin'
RABBIT_HOST = 'localhost'
RABBIT_PORT = 7672

TASKS_QUEUE = 'tasks'
WORKER_EVENTS_QUEUE = 'events.worker'
PLANNER_EVENTS_QUEUE = 'events.planner'
DATA_MANAGER_QUEUE = 'data_manager.requests'
DATA_MANAGER_RESPONSE_QUEUE = 'data_manager.responses'
COMPLETION_QUEUE = 'job_completion'  # новая очередь только для уведомлений
API_QUEUE = 'user_requests'  # API Gateway слушает эту очередь

NUM_REDUCE_PARTS = 4
BUCKET_NAME = "mapreduce"

main_tasks = {}
correlation_ids = {}  # для split и merge


def get_task(ch, method, properties, body):
    data = json.loads(body)
    print(f"[PLANNER] : got task {data}")

    MAIN_TASK_ID = data['task_id']
    INPUT_KEY = f"{MAIN_TASK_ID}/input_file/process.txt"

    request_split_from_data_manager(
        ch,
        input_key=INPUT_KEY,
        main_task_id=MAIN_TASK_ID,
        prefix=f"{MAIN_TASK_ID}/"
    )

    ch.basic_ack(delivery_tag=method.delivery_tag)


def send_task(ch, task_type: str, address: str, main_task_id: str, task_id: str,
              storage: str = "minio", bucket: str = "mapreduce"):
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
        properties=pika.BasicProperties(delivery_mode=2)
    )
    print(f"[Planner] sent {task_type} task {task_id} address={address}")


def request_split_from_data_manager(ch, input_key: str, main_task_id: str, prefix: str):
    correlation_id = str(uuid.uuid4())
    correlation_ids[correlation_id] = ("split", main_task_id)

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
    print(f"[Planner] requested split for {input_key}")


def request_merge_from_data_manager(ch, main_task_id: str):
    correlation_id = str(uuid.uuid4())
    correlation_ids[correlation_id] = ("merge", main_task_id)

    message = {
        "action": "merge_reduce",
        "main_task_id": main_task_id,
        "bucket": BUCKET_NAME,
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
    print(f"[Planner] requested merge_reduce for {main_task_id}")


def data_manager_response_callback(ch, method, properties, body):
    msg = json.loads(body)
    correlation_id = properties.correlation_id

    if correlation_id not in correlation_ids:
        print(f"[Planner] Unknown correlation_id {correlation_id}")
        ch.basic_ack(method.delivery_tag)
        return

    action_type, main_task_id = correlation_ids.pop(correlation_id)

    if action_type == "split":
        chunk_keys = msg.get("chunk_keys", [])
        if not chunk_keys:
            print(f"[Planner] Split failed for {main_task_id}")
            ch.basic_ack(method.delivery_tag)
            return

        print(f"[Planner] Received {len(chunk_keys)} chunks for {main_task_id}")

        main_tasks[main_task_id] = {
            "chunks": chunk_keys,
            "chunk_status": {k: 'pending' for k in chunk_keys},
            "task_map": {}
        }

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

        for key in chunk_keys:
            task_id = str(uuid.uuid4())
            main_tasks[main_task_id]['task_map'][task_id] = key
            send_task(ch, "map", key, main_task_id, task_id)

        print("[Planner] map tasks sent")

    elif action_type == "merge":
        if msg.get("status") == "success":
            result_key = msg["result_key"]
            print(f"[Planner] Merge completed: {result_key}")

            # Шлём в НОВУЮ очередь
            ch.basic_publish(
                exchange='',
                routing_key=COMPLETION_QUEUE,
                body=json.dumps({
                    "task_id": main_task_id,
                    "status": "completed",
                    "result_key": result_key
                }),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            print(f"[Planner] Notified about completion via {COMPLETION_QUEUE}")

    ch.basic_ack(method.delivery_tag)


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
            send_task(ch, "reduce", str(part), main_task_id, str(uuid.uuid4()))

    if event == "reduce.phase.done":
        main_task_id = msg.get("main_task_id")
        print(f"[Planner] Reduce phase completed for {main_task_id} — requesting merge")

        request_merge_from_data_manager(ch, main_task_id)

    ch.basic_ack(method.delivery_tag)


def start_listeners(channel):
    channel.basic_consume(queue=PLANNER_EVENTS_QUEUE, on_message_callback=planner_event_callback, auto_ack=False)
    channel.basic_consume(queue=DATA_MANAGER_RESPONSE_QUEUE, on_message_callback=data_manager_response_callback, auto_ack=False)
    channel.basic_consume(queue=API_QUEUE, on_message_callback=get_task, auto_ack=False)
    print("[Planner] Waiting for events...")
    channel.start_consuming()


def main():
    print("[PLANNER] started")
    credentials = pika.PlainCredentials(RABBIT_LOGIN, RABBIT_PASS)
    params = pika.ConnectionParameters(host=RABBIT_HOST, port=RABBIT_PORT, virtual_host='/', credentials=credentials)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()

    for q in [TASKS_QUEUE, WORKER_EVENTS_QUEUE, PLANNER_EVENTS_QUEUE, DATA_MANAGER_QUEUE, DATA_MANAGER_RESPONSE_QUEUE, API_QUEUE, COMPLETION_QUEUE]:
        ch.queue_declare(queue=q, durable=True)

    start_listeners(ch)


if __name__ == "__main__":
    main()
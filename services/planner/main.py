# planner.py  (replace your existing planner script content with this)
import pika
import json
import uuid
import time
import os

from storage_client import upload_file

RABBIT_PASS = 'password'
RABBIT_LOGIN = 'admin'
RABBIT_HOST = 'localhost'
RABBIT_PORT = 5672

TASKS_QUEUE = 'tasks'
WORKER_EVENTS_QUEUE = 'events.worker'   # consumed by Task Manager
PLANNER_EVENTS_QUEUE = 'events.planner' # planner listens here (from Task Manager)

NUM_REDUCE_PARTS = 4
BUCKET_NAME = "mapreduce"

# In-memory minimal state to support requeues on worker failure
# main_tasks[main_task_id] = {
#   'chunks': [s3_key,...],
#   'chunk_status': { s3_key: 'pending'|'done' },
#   'task_map': { task_id: s3_key }
# }
main_tasks = {}

def split_and_upload_txt(
    input_file: str,
    lines_per_file: int = 1_000_000,
    bucket: str = "mapreduce",
    prefix: str = ""
) -> list:
    """
    Split txt into chunks and upload them to S3.
    Returns list of S3 keys.
    """
    uploaded = []

    with open(input_file, 'r', encoding='utf-8') as f:
        part_idx = 0
        buffer = []

        for line in f:
            buffer.append(line)
            if len(buffer) >= lines_per_file:
                part_name = f"{os.path.basename(input_file)}_part{part_idx}.txt"
                with open(part_name, 'w', encoding='utf-8') as pf:
                    pf.writelines(buffer)

                key = f"{prefix}{part_name}"
                upload_file(part_name, bucket, key)
                uploaded.append(key)

                os.remove(part_name)
                buffer.clear()
                part_idx += 1

        if buffer:
            part_name = f"{os.path.basename(input_file)}_part{part_idx}.txt"
            with open(part_name, 'w', encoding='utf-8') as pf:
                pf.writelines(buffer)

            key = f"{prefix}{part_name}"
            upload_file(part_name, bucket, key)
            uploaded.append(key)

            os.remove(part_name)

    return uploaded


def send_task(
    ch,
    task_type: str,
    address: str,
    main_task_id: str,
    task_id: str,
    storage: str = "minio",
    bucket: str = "mapreduce"
):
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


def planner_event_callback(ch, method, properties, body):
    """
    This consumes events from Task Manager (events.planner).
    Events handled:
      - map.task.done  -> marks chunk done (payload: main_task_id, task_id)
      - map.phase.done -> (optional) TM may also send this, we use it as signal
      - reduce.phase.done -> job finished
      - worker.failed -> worker died, planner should requeue pending map tasks
    """
    msg = json.loads(body)
    event = msg.get("event")

    # 1) worker died -> requeue pending map tasks
    if event == "worker.failed":
        worker_id = msg.get("worker_id")
        print(f"[Planner] got worker.failed for worker={worker_id} — requeueing pending map tasks")
        # requeue any pending chunk tasks across main_tasks
        total_requeued = 0
        for main_task_id, info in main_tasks.items():
            for chunk_key, status in list(info['chunk_status'].items()):
                if status != 'done':
                    # create new task_id and send
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
                    # keep chunk_status as pending (still pending until map.task.done arrives)
                    total_requeued += 1
        print(f"[Planner] requeued {total_requeued} tasks due to worker failure")
        ch.basic_ack(method.delivery_tag)
        return

    # 2) individual map task completed (from TM forwarding worker's map.done)
    if event == "map.task.done" or event == "map.done":
        main_task_id = msg.get("main_task_id")
        task_id = msg.get("task_id")
        # translate task_id -> chunk and mark done
        if main_task_id in main_tasks:
            mapping = main_tasks[main_task_id]['task_map']
            chunk = mapping.get(task_id)
            if chunk:
                main_tasks[main_task_id]['chunk_status'][chunk] = 'done'
                print(f"[Planner] map task {task_id} done -> chunk {chunk} marked done")
            else:
                # maybe this was a requeued task with a different id not tracked? try to find by value
                # scan for mapping where value equals some chunk? (best-effort)
                found = False
                for tid, ck in list(mapping.items()):
                    if tid == task_id:
                        main_tasks[main_task_id]['chunk_status'][ck] = 'done'
                        found = True
                        break
                if not found:
                    print(f"[Planner] map.task.done for unknown task_id {task_id} (ignoring)")
        else:
            print(f"[Planner] map.task.done for unknown main_task_id {main_task_id} (ignoring)")

        ch.basic_ack(method.delivery_tag)
        return

    # 3) map phase fully completed signalled by TM
    if event == "map.phase.done":
        main_task_id = msg.get("main_task_id")
        print(f"[Planner] map phase completed for {main_task_id}")

        # send reduce.expected via worker events (TM also might need it)
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

        # send reduce tasks
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

    # unknown
    print(f"[Planner] unknown planner event: {event}")
    ch.basic_ack(method.delivery_tag)


def start_planner_event_listener(channel):
    channel.basic_consume(
        queue=PLANNER_EVENTS_QUEUE,
        on_message_callback=planner_event_callback,
        auto_ack=False
    )
    channel.start_consuming()


def main():
    credentials = pika.PlainCredentials(RABBIT_LOGIN, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PORT,
        virtual_host='/',
        credentials=credentials
    )

    conn = pika.BlockingConnection(params)
    ch = conn.channel()

    # queues
    ch.queue_declare(queue=TASKS_QUEUE, durable=True)
    ch.queue_declare(queue=WORKER_EVENTS_QUEUE, durable=True)
    ch.queue_declare(queue=PLANNER_EVENTS_QUEUE, durable=True)

    MAIN_TASK_ID = "1"
    INPUT_FILE = r"C:\ovr_pr\large_test_words.txt"

    print("[Planner] splitting input file...")
    files = split_and_upload_txt(
        INPUT_FILE,
        lines_per_file=1_000_000,
        bucket=BUCKET_NAME,
        prefix=f"{MAIN_TASK_ID}/"
    )

    print(f"[Planner] uploaded {len(files)} chunks")

    # initialize in-memory state
    main_tasks[MAIN_TASK_ID] = {
        "chunks": list(files),
        "chunk_status": {k: 'pending' for k in files},
        "task_map": {}  # task_id -> chunk_key
    }

    ch.basic_publish(
        exchange='',
        routing_key=WORKER_EVENTS_QUEUE,
        body=json.dumps({
            "event": "map.expected",
            "main_task_id": MAIN_TASK_ID,
            "count": len(files)
        }),
        properties=pika.BasicProperties(delivery_mode=2)
    )

    # send map tasks
    for key in files:
        task_id = str(uuid.uuid4())
        # store mapping before send
        main_tasks[MAIN_TASK_ID]['task_map'][task_id] = key
        send_task(
            ch,
            task_type="map",
            address=key,
            main_task_id=MAIN_TASK_ID,
            task_id=task_id,
            storage="minio",
            bucket=BUCKET_NAME
        )

    print("[Planner] map tasks sent")

    # listen events from Task Manager
    print("[Planner] waiting for Task Manager events...")
    start_planner_event_listener(ch)


if __name__ == "__main__":
    main()

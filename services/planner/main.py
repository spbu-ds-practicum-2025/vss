import pika
import json
import uuid
import time

QUEUE_NAME = 'tasks'

RABBIT_PASS = 'password'
RABBIT_LOGIN = 'admin'
RABBIT_HOST = 'localhost'
RABBIT_PORT = 5672

# undone: implement file splitting
def split_txt_file(input_file: str, lines_per_file: int = 1_000_000) -> list:
    ...


def send_map_task(ch, task_type: str, address: str):

    task = {
        "task_id": str(uuid.uuid4()),
        "type": task_type,
        "address": address,
        "created_at": time.time()
    }
    body = json.dumps(task)

    # delivery_mode=2 — сделать сообщение persistent
    props = pika.BasicProperties(delivery_mode=2, content_type='application/json')
    ch.basic_publish(exchange='', routing_key=QUEUE_NAME, body=body, properties=props)

    print(f"[Planner] sent task {task['task_id']} type={task_type}")


def send_reduce_task(ch, task_type: str, address: str, part_id: int):

    task = {
        "task_id": str(uuid.uuid4()),
        "type": task_type,
        "address": address,
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

    # send_map_task(ch, "map", r"C:\ovr_pr\large_test_words.txt")
    send_reduce_task(ch, "reduce", r"C:\ovr_pr\vss\services\worker\shuffle_files", part_id=0)

    conn.close()


if __name__ == "__main__":
    main()


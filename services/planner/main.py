import pika
import json
import uuid
import time

QUEUE_NAME = 'tasks'


def send_task(task_type: str, address: str):
    credentials = pika.PlainCredentials('admin', 'password')
    params = pika.ConnectionParameters(host='localhost', port=5672, virtual_host='/', credentials=credentials)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()

    # durable=True — очередь переживёт перезапуск брокера
    ch.queue_declare(queue=QUEUE_NAME, durable=True)

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
    conn.close()


if __name__ == "__main__":
    # Примеры отправки
    send_task("map", r"C:\ovr_pr\large_test_words.txt")


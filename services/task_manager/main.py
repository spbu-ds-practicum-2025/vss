# task_manager/main.py
import pika
import json
import time
import threading

RABBIT_PASS = 'password'
RABBIT_LOGIN = 'admin'
RABBIT_HOST = 'localhost'
RABBIT_PORT = 5672

HEARTBEAT_QUEUE = 'worker.heartbeat'
DEAD_EVENT_QUEUE = 'worker.dead'

HEARTBEAT_TIMEOUT = 10  # секунд

workers_last_seen = {}
lock = threading.Lock()


def heartbeat_callback(ch, method, properties, body):
    try:
        msg = json.loads(body)
        worker_id = msg['worker_id']
        ts = msg['ts']
    except Exception:
        ch.basic_ack(method.delivery_tag)
        return

    with lock:
        workers_last_seen[worker_id] = ts

    ch.basic_ack(method.delivery_tag)


def monitor_loop(ch):
    while True:
        now = time.time()
        dead = []

        with lock:
            for worker_id, last in list(workers_last_seen.items()):
                if now - last > HEARTBEAT_TIMEOUT:
                    dead.append(worker_id)
                    del workers_last_seen[worker_id]

        for worker_id in dead:
            event = {
                "worker_id": worker_id,
                "detected_at": now
            }
            ch.basic_publish(
                exchange='',
                routing_key=DEAD_EVENT_QUEUE,
                body=json.dumps(event),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            print(f"[TaskManager] worker dead: {worker_id}")

        time.sleep(2)


def main():
    creds = pika.PlainCredentials(RABBIT_LOGIN, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PORT,
        credentials=creds
    )

    conn = pika.BlockingConnection(params)
    ch = conn.channel()

    ch.queue_declare(queue=HEARTBEAT_QUEUE, durable=False)
    ch.queue_declare(queue=DEAD_EVENT_QUEUE, durable=True)

    ch.basic_consume(
        queue=HEARTBEAT_QUEUE,
        on_message_callback=heartbeat_callback,
        auto_ack=False
    )

    t = threading.Thread(target=monitor_loop, args=(ch,), daemon=True)
    t.start()

    print("[TaskManager] started")
    ch.start_consuming()


if __name__ == "__main__":
    main()

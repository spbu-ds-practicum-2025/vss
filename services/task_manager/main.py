# task_manager.py
import pika
import json
import time
import threading
import redis
from datetime import datetime, timedelta
from collections import defaultdict

RABBIT_PASS = 'password'
RABBIT_LOGIN = 'admin'
RABBIT_HOST = 'localhost'
RABBIT_PORT = 7672

# Redis config
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = None

WORKER_EVENTS_QUEUE = 'events.worker'
PLANNER_EVENTS_QUEUE = 'events.planner'

HEARTBEAT_TIMEOUT = 30
HEARTBEAT_CHECK_INTERVAL = 10
DEAD_WORKER_TTL = 86400 * 7  # 7 дней храним мёртвых воркеров

reduce_expected = defaultdict(int)
reduce_received = defaultdict(int)
reduce_phase_done_sent = set()

# Redis connection pool
redis_pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD)
def get_redis():
    return redis.Redis(connection_pool=redis_pool)

# Ключи
def alive_worker_key(wid): return f"worker:{wid}"
def dead_worker_key(wid): return f"dead_worker:{wid}"
def job_key(mtid): return f"job:{mtid}"
def phase_done_set(): return "map_phase_done_sent"


def format_time(ts):
    return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


def move_to_graveyard(r, wid, now_str, failed_task=None):
    """Перемещает воркера в 'кладбище' мёртвых"""
    alive_key = alive_worker_key(wid)
    dead_key = dead_worker_key(wid)

    # Копируем все данные
    data = r.hgetall(alive_key)
    if data:
        r.hset(dead_key, mapping={k.decode(): v.decode() for k, v in data.items()})
        r.hset(dead_key, "status", "dead")
        r.hset(dead_key, "death_time", now_str)
        if failed_task:
            r.hset(dead_key, "failed_task_id", failed_task.get("task_id", ""))
            r.hset(dead_key, "failed_main_task_id", failed_task.get("main_task_id", ""))
            r.hset(dead_key, "failed_address", failed_task.get("address", ""))
            r.hset(dead_key, "failed_type", failed_task.get("type", ""))

        r.expire(dead_key, DEAD_WORKER_TTL)

    # Удаляем из живых
    r.delete(alive_key)
    print(f"[TM] Worker {wid} moved to graveyard (dead_worker:{wid})")


def handle_worker_event(ch, method, properties, body):
    try:
        msg = json.loads(body)
    except Exception:
        ch.basic_ack(method.delivery_tag)
        return

    ev = msg.get("event")
    now = time.time()
    now_str = format_time(now)
    r = get_redis()
    wid = msg.get("worker_id")

    if ev == "heartbeat":
        if not wid:
            ch.basic_ack(method.delivery_tag)
            return

        pipe = r.pipeline()
        pipe.hset(alive_worker_key(wid), mapping={
            "last_heartbeat": now_str,
            "status": "alive",
            "updated_at": now_str
        })

        current_task = msg.get("current_task")
        if current_task:
            pipe.hset(alive_worker_key(wid), mapping={
                "current_task_id": current_task.get("task_id", ""),
                "current_main_task_id": current_task.get("main_task_id", ""),
                "current_address": current_task.get("address", ""),
                "current_type": current_task.get("type", "")
            })

        pipe.expire(alive_worker_key(wid), DEAD_WORKER_TTL * 2)  # большой TTL для живых
        pipe.execute()

        task_id = current_task.get("task_id", "idle") if current_task else "idle"
        print(f"[TM] heartbeat from {wid} | task: {task_id} | time: {now_str}")

    elif ev == "task.started":
        if not wid:
            ch.basic_ack(method.delivery_tag)
            return
        task = msg.get("task", {})
        r.hset(alive_worker_key(wid), mapping={
            "current_task_id": task.get("task_id", ""),
            "current_main_task_id": task.get("main_task_id", ""),
            "current_address": task.get("address", ""),
            "current_type": task.get("type", ""),
            "status": "busy",
            "last_heartbeat": now_str
        })

    elif ev == "task.done":
        if wid:
            r.hdel(alive_worker_key(wid), "current_task_id", "current_main_task_id", "current_address", "current_type")
            r.hset(alive_worker_key(wid), "status", "alive")

    elif ev == "map.expected":
        mtid = msg.get("main_task_id")
        cnt = int(msg.get("count", 0))
        r.hset(job_key(mtid), mapping={"expected": cnt, "received": 0})
        print(f"[TM] map.expected: {mtid} -> {cnt}")

    elif ev in ("map.done", "map.task.done"):
        mtid = msg.get("main_task_id")
        tid = msg.get("task_id")

        ch.basic_publish(
            exchange='',
            routing_key=PLANNER_EVENTS_QUEUE,
            body=json.dumps({
                "event": "map.task.done",
                "main_task_id": mtid,
                "task_id": tid,
                "time": now
            }),
            properties=pika.BasicProperties(delivery_mode=2)
        )

        if mtid:
            received = r.hincrby(job_key(mtid), "received", 1)
            expected = int(r.hget(job_key(mtid), "expected") or 0)

            print(f"[TM] map.done received for {mtid}: {received}/{expected}")

            already_sent = r.sismember(phase_done_set(), mtid)
            if expected > 0 and received >= expected and not already_sent:
                ch.basic_publish(
                    exchange='',
                    routing_key=PLANNER_EVENTS_QUEUE,
                    body=json.dumps({"event": "map.phase.done", "main_task_id": mtid}),
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                r.sadd(phase_done_set(), mtid)
                print(f"[TM] published map.phase.done for {mtid}")

                # Очистка job-метаданных
                r.delete(job_key(mtid))
                r.srem(phase_done_set(), mtid)
                print(f"[TM] Cleaned up job metadata for {mtid}")
                
    elif ev == "reduce.expected":
        mtid = msg.get("main_task_id")
        cnt = int(msg.get("count", 0))
        reduce_expected[mtid] = cnt
        reduce_received[mtid] = 0
        print(f"[TM] reduce.expected: {mtid} -> {cnt}")

    elif ev == "reduce.done":
        mtid = msg.get("main_task_id")
        if mtid:
            reduce_received[mtid] += 1
            print(f"[TM] reduce.done received for {mtid}: {reduce_received[mtid]}/{reduce_expected.get(mtid, '?')}")

            if (reduce_expected.get(mtid, 0) > 0
                    and reduce_received[mtid] >= reduce_expected[mtid]
                    and mtid not in reduce_phase_done_sent):
                notify = {"event": "reduce.phase.done", "main_task_id": mtid}
                ch.basic_publish(
                    exchange='',
                    routing_key=PLANNER_EVENTS_QUEUE,
                    body=json.dumps(notify),
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                reduce_phase_done_sent.add(mtid)
                print(f"[TM] published reduce.phase.done for {mtid}")

    ch.basic_ack(method.delivery_tag)


def monitor_heartbeats(ch):
    r = get_redis()
    processed_dead = set()

    while True:
        now = time.time()
        now_str = format_time(now)
        worker_keys = r.keys("worker:*")  # только живые

        for key_b in worker_keys:
            key = key_b.decode()
            wid = key.split(":", 1)[1]

            last_hb_str = r.hget(key, "last_heartbeat")
            if not last_hb_str:
                continue
            last_hb = datetime.strptime(last_hb_str.decode(), '%Y-%m-%d %H:%M:%S').timestamp()

            if now - last_hb > HEARTBEAT_TIMEOUT:
                if wid in processed_dead:
                    continue

                print(f"[TM] Worker {wid} considered DEAD (last heartbeat: {last_hb_str.decode()})")

                # Получаем текущую задачу
                current_task_id = r.hget(key, "current_task_id")
                current_main_task_id = r.hget(key, "current_main_task_id")
                current_address = r.hget(key, "current_address")
                current_type = r.hget(key, "current_type")

                failed_task = None
                if current_task_id:
                    failed_task = {
                        "task_id": current_task_id.decode(),
                        "main_task_id": current_main_task_id.decode() if current_main_task_id else "",
                        "address": current_address.decode() if current_address else "",
                        "type": current_type.decode() if current_type else ""
                    }

                # Уведомляем Planner
                payload = {"event": "worker.failed", "worker_id": wid, "time": now}
                if failed_task:
                    payload["failed_task"] = failed_task

                ch.basic_publish(
                    exchange='',
                    routing_key=PLANNER_EVENTS_QUEUE,
                    body=json.dumps(payload),
                    properties=pika.BasicProperties(delivery_mode=2)
                )

                # Перемещаем в могилку
                move_to_graveyard(r, wid, now_str, failed_task)

                processed_dead.add(wid)

        time.sleep(HEARTBEAT_CHECK_INTERVAL)


def cleanup_old_dead_workers():
    """Фоновая задача: удаляет мёртвых воркеров старше 7 дней"""
    r = get_redis()
    while True:
        now = time.time()
        dead_keys = r.keys("dead_worker:*")
        for key_b in dead_keys:
            key = key_b.decode()
            wid = key.split(":", 1)[1]
            death_time_str = r.hget(key, "death_time")
            if death_time_str:
                death_time = datetime.strptime(death_time_str.decode(), '%Y-%m-%d %H:%M:%S').timestamp()
                if now - death_time > DEAD_WORKER_TTL:
                    r.delete(key)
                    print(f"[TM] Cleaned up old dead worker {wid}")

        time.sleep(3600)  # раз в час


def recover_state_on_startup(ch):
    r = get_redis()
    now = time.time()
    now_str = format_time(now)
    worker_keys = r.keys("worker:*")

    print(f"[TM] Recovering state: found {len(worker_keys)} alive worker records")

    for key_b in worker_keys:
        key = key_b.decode()
        wid = key.split(":", 1)[1]

        last_hb_str = r.hget(key, "last_heartbeat")
        if not last_hb_str:
            continue
        last_hb = datetime.strptime(last_hb_str.decode(), '%Y-%m-%d %H:%M:%S').timestamp()

        if now - last_hb > HEARTBEAT_TIMEOUT:
            print(f"[TM] Recovery: Worker {wid} was dead before restart — notifying planner")

            current_task_id = r.hget(key, "current_task_id")
            current_main_task_id = r.hget(key, "current_main_task_id")
            current_address = r.hget(key, "current_address")
            current_type = r.hget(key, "current_type")

            failed_task = None
            if current_task_id:
                failed_task = {
                    "task_id": current_task_id.decode(),
                    "main_task_id": current_main_task_id.decode() if current_main_task_id else "",
                    "address": current_address.decode() if current_address else "",
                    "type": current_type.decode() if current_type else ""
                }

            payload = {"event": "worker.failed", "worker_id": wid, "time": now}
            if failed_task:
                payload["failed_task"] = failed_task

            ch.basic_publish(
                exchange='',
                routing_key=PLANNER_EVENTS_QUEUE,
                body=json.dumps(payload),
                properties=pika.BasicProperties(delivery_mode=2)
            )

            move_to_graveyard(r, wid, now_str, failed_task)


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

    ch.queue_declare(queue=WORKER_EVENTS_QUEUE, durable=True)
    ch.queue_declare(queue=PLANNER_EVENTS_QUEUE, durable=True)

    # Восстановление при старте
    recover_state_on_startup(ch)

    # Запуск мониторинга живых воркеров
    monitor_thread = threading.Thread(target=monitor_heartbeats, args=(ch,), daemon=True)
    monitor_thread.start()

    # Запуск очистки старых мёртвых воркеров
    cleanup_thread = threading.Thread(target=cleanup_old_dead_workers, daemon=True)
    cleanup_thread.start()

    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=WORKER_EVENTS_QUEUE, on_message_callback=handle_worker_event, auto_ack=False)

    print("[TM] Task Manager started — with worker graveyard and auto-cleanup")
    print("[TM] Alive workers: keys worker:* | Dead workers: keys dead_worker:*")
    print("[TM] Waiting for worker events...")
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        ch.stop_consuming()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
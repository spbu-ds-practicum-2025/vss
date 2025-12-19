# task_manager.py
import pika
import json
import time
import threading
from collections import defaultdict

RABBIT_PASS = 'password'
RABBIT_LOGIN = 'admin'
RABBIT_HOST = 'localhost'
RABBIT_PORT = 7672

WORKER_EVENTS_QUEUE = 'events.worker'    # incoming: heartbeats, map.done, map.expected (from planner)
PLANNER_EVENTS_QUEUE = 'events.planner'  # outgoing -> planner

HEARTBEAT_TIMEOUT = 30  # seconds
HEARTBEAT_CHECK_INTERVAL = 10

# state
worker_last_seen = {}  # worker_id -> last_timestamp
# map tracking
map_expected = defaultdict(int)      # main_task_id -> expected count
map_received = defaultdict(int)      # main_task_id -> received count
# to avoid duplicate map.phase.done sends
map_phase_done_sent = set()


def handle_worker_event(ch, method, properties, body):
    """
    Accepts messages published to events.worker:
      - heartbeat: {"event":"heartbeat","worker_id":...}
      - map.expected: {"event":"map.expected","main_task_id":...,"count":n}
      - map.done: {"event":"map.done","main_task_id":...,"task_id":...}
    Relays map.done to planner as map.task.done, and publishes map.phase.done when ready.
    """
    try:
        msg = json.loads(body)
    except Exception:
        ch.basic_ack(method.delivery_tag)
        return

    ev = msg.get("event")
    now = time.time()

    if ev == "heartbeat":
        wid = msg.get("worker_id")
        worker_last_seen[wid] = now
        # optionally track other worker metadata
        # echo/log
        # print(f"[TM] heartbeat from {wid}")
    elif ev == "map.expected":
        mtid = msg.get("main_task_id")
        cnt = int(msg.get("count", 0))
        map_expected[mtid] = cnt
        map_received[mtid] = map_received.get(mtid, 0)
        print(f"[TM] map.expected: main_task_id={mtid} expected={cnt}")
    elif ev == "map.done" or ev == "map.task.done":
        mtid = msg.get("main_task_id")
        tid = msg.get("task_id")
        # forward to planner as map.task.done
        forward = {
            "event": "map.task.done",
            "main_task_id": mtid,
            "task_id": tid,
            "time": now
        }
        ch.basic_publish(exchange='', routing_key=PLANNER_EVENTS_QUEUE, body=json.dumps(forward),
                         properties=pika.BasicProperties(delivery_mode=2))
        # update counters and maybe publish map.phase.done
        if mtid:
            map_received[mtid] = map_received.get(mtid, 0) + 1
            print(f"[TM] map.done received for {mtid}: {map_received[mtid]}/{map_expected.get(mtid, '?')}")
            if map_expected.get(mtid, 0) > 0 and map_received[mtid] >= map_expected[mtid] and mtid not in map_phase_done_sent:
                # notify planner that full map phase completed for this main task
                notify = {"event": "map.phase.done", "main_task_id": mtid}
                ch.basic_publish(exchange='', routing_key=PLANNER_EVENTS_QUEUE, body=json.dumps(notify),
                                 properties=pika.BasicProperties(delivery_mode=2))
                map_phase_done_sent.add(mtid)
                print(f"[TM] published map.phase.done for {mtid}")
    else:
        # unknown event — ignore for now
        pass

    ch.basic_ack(method.delivery_tag)


def monitor_heartbeats(ch):
    # This thread periodically scans worker_last_seen and when worker considered dead,
    # logs to console and publishes worker.failed to planner.
    while True:
        now = time.time()
        to_kill = []
        for wid, ts in list(worker_last_seen.items()):
            if now - ts > HEARTBEAT_TIMEOUT:
                to_kill.append(wid)

        for wid in to_kill:
            # log loudly
            print(f"[TM] Worker {wid} considered DEAD (last seen {int(now - worker_last_seen[wid])}s ago)")

            # notify planner
            payload = {
                "event": "worker.failed",
                "worker_id": wid,
                "time": now
                # we intentionally do not include task list here — planner will requeue pending chunks
            }
            ch.basic_publish(exchange='', routing_key=PLANNER_EVENTS_QUEUE, body=json.dumps(payload),
                         properties=pika.BasicProperties(delivery_mode=2))
            # drop worker from tracking so we don't repeat
            del worker_last_seen[wid]

        time.sleep(HEARTBEAT_CHECK_INTERVAL)


def main():
    credentials = pika.PlainCredentials(RABBIT_LOGIN, RABBIT_PASS)
    params = pika.ConnectionParameters(host=RABBIT_HOST, port=RABBIT_PORT, virtual_host='/', credentials=credentials)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()

    ch.queue_declare(queue=WORKER_EVENTS_QUEUE, durable=True)
    ch.queue_declare(queue=PLANNER_EVENTS_QUEUE, durable=True)

    # start heartbeat monitor thread (uses same channel to publish; that's fine)
    t = threading.Thread(target=monitor_heartbeats, args=(ch,), daemon=True)
    t.start()

    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=WORKER_EVENTS_QUEUE, on_message_callback=handle_worker_event, auto_ack=False)

    print("[TM] Task Manager started, waiting for worker events...")
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        ch.stop_consuming()
    finally:
        conn.close()


if __name__ == "__main__":
    main()

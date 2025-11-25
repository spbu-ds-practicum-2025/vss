from worker import WordCountMapper, MapExecutor, WordCountShuffler, ShuffleExecutor, WordCountReducer, ReduceExecutor, DataManager
from loaders import txtDataSource, jsonDataSink, txtDataSink, jsonDataSource

import shutil
import os
import pika
import json
import uuid

QUEUE_NAME = 'tasks'
WORKER_ID = "1"

RABBIT_PASS = 'password'
RABBIT_LOGIN = 'admin'
RABBIT_HOST = 'localhost'
RABBIT_PORT = 5672

# numsber of retries for failed tasks
MAX_RETRIES = 3

def callback(ch, method, properties, body):
    # if message is not valid JSON — ack and skip
    try:
        task = json.loads(body)
    except Exception:
        print(f"[{WORKER_ID}] invalid message, ack and skip")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    print(f"[{WORKER_ID}] picked {task.get('task_id')}")

    SPILL_FILES_DIR = rf"storage\{task.get('task_id')}\spill_files"
    SHUFFLE_FILES_DIR = rf"storage\{task.get('task_id')}\shuffle_files"
    REDUCE_OUTPUT_DIR = rf"storage\{task.get('task_id')}\reduce_output"


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


    def process_reduce_task(file_address: str, part_num: int):
        # reducing phase

        if not os.path.exists(SPILL_FILES_DIR):
            raise FileNotFoundError(f"Spill files dir not found: {SPILL_FILES_DIR}")

        reducer = WordCountReducer()
        reduce_executor = ReduceExecutor(reducer,
                                        sink=jsonDataSink(REDUCE_OUTPUT_DIR, mode="jsonl"),
                                        source=jsonDataSource())
        
        reduce_executor.process(part_dir=SHUFFLE_FILES_DIR, part_num=part_num)

        print("Reducing phase completed.")
        shutil.rmtree(SHUFFLE_FILES_DIR)

    headers = {}
    if properties is not None:
        # properties.headers может быть None или dict
        headers = properties.headers or {}
    attempts = int(headers.get('x-attempts', 0))

    try:
        if task.get('type') == 'map':
            process_map_task(task.get('address'))
            ch.basic_ack(delivery_tag=method.delivery_tag) 
            print(f"[{WORKER_ID}] completed {task.get('task_id')} type=map")
        elif task.get('type') == 'reduce':
            process_reduce_task(task.get('address'), task.get('part_id'))
            ch.basic_ack(delivery_tag=method.delivery_tag) 
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



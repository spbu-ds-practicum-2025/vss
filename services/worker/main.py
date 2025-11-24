from worker import WordCountMapper, MapExecutor, WordCountShuffler, ShuffleExecutor, WordCountReducer, ReduceExcecutor, DataManager
from loaders import txtDataSource, jsonDataSink, txtDataSink, jsonDataSource

import shutil
import os
import pika
import json
import uuid

QUEUE_NAME = 'tasks'
WORKER_ID = "1"


def process_task(file_address: str):
    # cleanup previous run directories
    dirs = [r'C:\ovr_pr\vss\services\worker\spill_files',
            r'C:\ovr_pr\vss\services\worker\shuffle_files',
            r'C:\ovr_pr\vss\services\worker\reduce_output']
    
    for d in dirs:
        if os.path.exists(d):
            shutil.rmtree(d)

    # mapping phase
    mapper = WordCountMapper()
    data_spill_saver = jsonDataSink(r"C:\ovr_pr\vss\services\worker\spill_files", mode="jsonl")
    data_sorce = txtDataSource()
    
    map_executor = MapExecutor(mapper, data_spill_saver, data_sorce, threshold=5_000)
    map_executor.process(filepath=file_address)

    print("Mapping phase completed. Starting shuffling phase...")

    # shuffling phase

    shuffler = WordCountShuffler(num_parts=4, flush_threshold=2_000)
    shuffle_executor = ShuffleExecutor(shuffler, 
                                       source=jsonDataSource(),
                                        sink=jsonDataSink(r"C:\ovr_pr\vss\services\worker\shuffle_files", mode="jsonl"))
    shuffle_executor.process(r"C:\ovr_pr\vss\services\worker\spill_files")
    print("Shuffling phase completed.")
    

def main():
    credentials = pika.PlainCredentials('admin', 'password')
    params = pika.ConnectionParameters(host='localhost', port=5672, virtual_host='/', credentials=credentials)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()

    ch.queue_declare(queue=QUEUE_NAME, durable=True)
    ch.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        # if message is not valid JSON — ack and skip
        try:
            task = json.loads(body)
        except Exception:
            print(f"[{WORKER_ID}] invalid message, ack and skip")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        print(f"[{WORKER_ID}] picked {task.get('task_id')}")

        try:
            process_task(task.get('address'))
            ch.basic_ack(delivery_tag=method.delivery_tag) 
            print(f"[{WORKER_ID}] completed {task.get('task_id')}")
        except Exception as e:
            print(f"[{WORKER_ID}] error processing {task.get('task_id')}: {e}")
            # В простом варианте: nack и requeue=True (вернётся в очередь)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

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




    # reducing phase
    # reducer = WordCountReducer()
    # reduce_executor = ReduceExcecutor(reducer,
    #                                   sink=jsonDataSink(r"C:\ovr_pr\worker\reduce_output", mode="jsonl"),
    #                                   source=jsonDataSource())
    # for part_num in range(4):
    #     reduce_executor.process(part_dir=r"C:\ovr_pr\worker\shuffle_files", part_num=part_num)

    # print("Reducing phase completed.")

    # # data management phase

    # dirs = [r'C:\ovr_pr\worker\spill_files',
    #     r'C:\ovr_pr\worker\shuffle_files']
    
    # for d in dirs:
    #     if os.path.exists(d):
    #         shutil.rmtree(d)
    
    # DataManager.manage_reduce_data(source=jsonDataSource(), sink=txtDataSink(r"C:\ovr_pr\worker\final_output"), dirpath=r"C:\ovr_pr\worker\reduce_output")

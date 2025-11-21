from worker import WordCountMapper, MapExecutor, WordCountShuffler, ShuffleExecutor, WordCountReducer, ReduceExcecutor, DataManager
from loaders import txtDataSource, jsonDataSink, txtDataSink, jsonDataSource
from pathlib import Path

import shutil, time
import os


def wait_for_file(file_path: str, check_interval: float = 5.0):
    path = Path(file_path)

    
    while not path.exists():
        time.sleep(check_interval)
    
    return True

if __name__ == "__main__":
    while True:
        # cleanup previous run directories
        dirs = [r'worker\spill_files',
                r'worker\shuffle_files',
                r'worker\reduce_output']
        
        for d in dirs:
            if os.path.exists(d):
                shutil.rmtree(d)

        file_path = os.path.join(os.getcwd(),'process.txt')
        wait_for_file(file_path)

        # mapping phase
        mapper = WordCountMapper()
        data_spill_saver = jsonDataSink(r"worker\spill_files", mode="jsonl")
        data_sorce = txtDataSource()
        
        map_executor = MapExecutor(mapper, data_spill_saver, data_sorce, threshold=5_000)
        map_executor.process(file_path)

        print("Mapping phase completed. Starting shuffling phase...")

        # shuffling phase

        shuffler = WordCountShuffler(num_parts=4, flush_threshold=2_000)
        shuffle_executor = ShuffleExecutor(shuffler, 
                                        source=jsonDataSource(),
                                            sink=jsonDataSink(r"worker\shuffle_files", mode="jsonl"))
        shuffle_executor.process(r"worker\spill_files")
        print("Shuffling phase completed.")

        # reducing phase
        reducer = WordCountReducer()
        reduce_executor = ReduceExcecutor(reducer,
                                        sink=jsonDataSink(r"worker\reduce_output", mode="jsonl"),
                                        source=jsonDataSource())
        for part_num in range(4):
            reduce_executor.process(part_dir=r"worker\shuffle_files", part_num=part_num)

        print("Reducing phase completed.")

        # data management phase

        dirs = [r'worker\spill_files',
            r'worker\shuffle_files']
        
        for d in dirs:
            if os.path.exists(d):
                shutil.rmtree(d)
        
        DataManager.manage_reduce_data(source=jsonDataSource(), sink=txtDataSink(os.getcwd()), dirpath=r"worker\reduce_output")
        os.remove('process.txt')

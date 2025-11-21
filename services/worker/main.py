from worker import WordCountMapper, MapExecutor, WordCountShuffler, ShuffleExecutor, WordCountReducer, ReduceExcecutor, DataManager
from loaders import txtDataSource, jsonDataSink, txtDataSink, jsonDataSource

import shutil
import os


if __name__ == "__main__":

    # cleanup previous run directories
    dirs = [r'C:\ovr_pr\worker\spill_files',
            r'C:\ovr_pr\worker\shuffle_files',
            r'C:\ovr_pr\worker\reduce_output']
    
    for d in dirs:
        if os.path.exists(d):
            shutil.rmtree(d)

    # mapping phase
    mapper = WordCountMapper()
    data_spill_saver = jsonDataSink(r"C:\ovr_pr\worker\spill_files", mode="jsonl")
    data_sorce = txtDataSource()
    
    map_executor = MapExecutor(mapper, data_spill_saver, data_sorce, threshold=5_000)
    map_executor.process(filepath=r"C:\ovr_pr\large_test_words.txt")

    print("Mapping phase completed. Starting shuffling phase...")

    # shuffling phase

    shuffler = WordCountShuffler(num_parts=4, flush_threshold=2_000)
    shuffle_executor = ShuffleExecutor(shuffler, 
                                       source=jsonDataSource(),
                                        sink=jsonDataSink(r"C:\ovr_pr\worker\shuffle_files", mode="jsonl"))
    shuffle_executor.process(r"C:\ovr_pr\worker\spill_files")
    print("Shuffling phase completed.")

    # reducing phase
    reducer = WordCountReducer()
    reduce_executor = ReduceExcecutor(reducer,
                                      sink=jsonDataSink(r"C:\ovr_pr\worker\reduce_output", mode="jsonl"),
                                      source=jsonDataSource())
    for part_num in range(4):
        reduce_executor.process(part_dir=r"C:\ovr_pr\worker\shuffle_files", part_num=part_num)

    print("Reducing phase completed.")

    # data management phase

    dirs = [r'C:\ovr_pr\worker\spill_files',
        r'C:\ovr_pr\worker\shuffle_files']
    
    for d in dirs:
        if os.path.exists(d):
            shutil.rmtree(d)
    
    DataManager.manage_reduce_data(source=jsonDataSource(), sink=txtDataSink(r"C:\ovr_pr\worker\final_output"), dirpath=r"C:\ovr_pr\worker\reduce_output")

from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Iterable, Any
import os
import zlib
import re
import unicodedata

from loaders import DataSource, DataSink


class Mapper(ABC):
    @staticmethod
    @abstractmethod
    def do_map(data) -> Iterable[tuple[Any, Any]]:
        pass


class Reducer(ABC):
    @staticmethod
    @abstractmethod
    def do_reduce(mapped_data):
        pass


class HealthCheck(ABC):
    '''
     Interface for health check
    '''
    @abstractmethod
    def is_healthy(self):
        pass


class WordCountMapper(Mapper):
    '''
        Mapper for word count
    '''
    token_pattern = re.compile(r"\w+", re.UNICODE)

    @staticmethod
    def do_map(line: str):
        line = unicodedata.normalize('NFKC', line)
        for w in WordCountMapper.token_pattern.findall(line):
            yield (w.lower(), 1)


class WordCountReducer(Reducer):
    '''
        Reducer for word count
    '''
    @staticmethod
    def do_reduce(mapped_data):
        reduced_data = {}
        for partial_result in mapped_data:
            for word, count in partial_result.items():
                reduced_data[word] = reduced_data.get(word, 0) + count
        return reduced_data

        
class MapExecutor:
    '''
        Executor for the mapping phase
    '''
    def __init__(self, worker:Mapper, sink:DataSink, source:DataSource, threshold:int=50_000):
        self.worker = worker
        self.sink = sink
        self.source = source
        self.threshold = threshold
        self.buffer = {}
        self.counter = 0

    def process(self, filepath:str) -> None:
        datasource = self.source.load(filepath)
        for line in datasource:
            for word, count in self.worker.do_map(line):
                self.buffer[word] = self.buffer.get(word, 0) + count

                if len(self.buffer) >= self.threshold:
                    self.sink.save(self.buffer, f"spill_{self.counter}")
                    self.counter += 1
                    self.buffer = {}

        if self.buffer:
            self.sink.save(self.buffer, f"spill_{self.counter}")
            self.counter += 1


class WordCountShuffler:
    def __init__(self, num_parts=4, flush_threshold=50_000):
        self.num_parts = num_parts
        self.flush_threshold = flush_threshold
        self.buffers = [defaultdict(int) for _ in range(num_parts)]
        self.part_counters = [0]*num_parts  # чтобы именовать файлы по частям

    def add_record(self, word, count, sink):
        bucket = self._stable_bucket(word, self.num_parts)
        buf = self.buffers[bucket]
        buf[word] += count
        if len(buf) >= self.flush_threshold:
            if sink is None:
                raise RuntimeError("sink required for flush")
            sink.save(dict(buf), name=f"part_{bucket}_{self.part_counters[bucket]}")
            self.part_counters[bucket] += 1
            buf.clear()

    def flush(self, sink):
        for i, data in enumerate(self.buffers):
            if data:
                sink.save(dict(data), name=f"part_{i}_{self.part_counters[i]}")
                self.part_counters[i] += 1
                data.clear()

    @staticmethod
    def _stable_bucket(key: str, num_parts: int) -> int:
        ''' Returns a stable bucket index for the given key.
         by hashing the key using zlib.crc32.'''
        return (zlib.crc32(key.encode('utf-8')) & 0xffffffff) % num_parts


class ShuffleExecutor:
    def __init__(self, shuffler:WordCountShuffler, sink:DataSink, source:DataSource):
        self.shuffler = shuffler
        self.sink = sink
        self.source = source

    def process(self, spill_dir: str) -> None:
        for file in os.listdir(spill_dir):  
            if file.startswith("spill_"):
                filepath = os.path.join(spill_dir, file)
                for record in self.source.load(filepath):
                    for word, count in record.items():
                        self.shuffler.add_record(word, count, self.sink)

        self.shuffler.flush(self.sink)


class ReduceExcecutor:
    '''
        Executor for the reducing phase
    '''
    def __init__(self, worker:Reducer, sink:DataSink, source:DataSource):
        self.worker = worker
        self.sink = sink
        self.source = source

    def process(self, part_dir:str, part_num:int) -> None:
        mapped_data = []
        for file in os.listdir(part_dir):  
            if file.startswith(f"part_{part_num}"):
                filepath = os.path.join(part_dir, file)
                for record in self.source.load(filepath):
                    mapped_data.append(record)
                
        reduced_data = self.worker.do_reduce(mapped_data)
        self.sink.save(reduced_data, name=f'reduced_{part_num}')
            

class DataManager:

    @classmethod
    def manage_reduce_data(cls, source, sink, dirpath:str) -> dict:
        '''
            Manages reduced data by combining all reduced parts into a single dictionary.
        '''
        combined_data = {}
        for file in os.listdir(dirpath):
            filepath = os.path.join(dirpath, file)
            for record in source.load(filepath):
                combined_data.update(record)

        combined_data = dict(sorted(combined_data.items(), key=lambda item: item[0]))
        sink.save(combined_data, name='result')
        return combined_data
    
    

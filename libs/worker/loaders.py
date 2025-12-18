from abc import ABC, abstractmethod
from typing import Generator, Iterable
import json
import os


class DataSource(ABC):
    '''
     Interface for loading data
    '''
    @classmethod
    @abstractmethod
    def load(cls, filepath: str) -> Iterable:
        pass


class DataSink(ABC):
    '''
     Interface for saving data
    '''
    def __init__(self, dirpath: str):
        self.dirpath = dirpath

    @abstractmethod
    def save(self, data, name:str) -> None:
        pass


class txtDataSource(DataSource):
    '''
        Data source for loading txt files
    '''
    @classmethod
    def load(cls, filepath: str) -> Generator[str, None, None]:
        with open(filepath, 'r', errors='ignore', encoding='utf-8') as file:
            for line in file:
                yield line.strip()


class jsonDataSource(DataSource):
    '''
        Data source for loading json files
    '''
    @classmethod
    def load(cls, filepath: str) -> Generator[dict, None, None]:
        with open(filepath, 'r', encoding='utf-8') as file:
            for line in file:
                yield json.loads(line)


class jsonDataSink(DataSink):
    '''
        Data sink for saving data to json files
    '''
    def __init__(self, dirpath: str, mode: str = "json"):
        self.dirpath = dirpath
        os.makedirs(dirpath, exist_ok=True)
        self.mode = mode

    def save(self, data: dict, name) -> None:
        '''
        Writes data to a json or jsonl file based on the specified mode.
        1. In "json" mode, saves the entire dictionary as a single JSON object
        2. In "jsonl" mode, saves each key-value pair as a separate JSON object on a new line.
        3. Increments a counter to ensure unique filenames for each save operation.
        '''
        if self.mode == "json":
            filepath = os.path.join(self.dirpath, f"{name}.json")
            with open(filepath, 'w') as f:
                json.dump(data, f)
    
        elif self.mode == "jsonl":
            filepath = os.path.join(self.dirpath, f"{name}.jsonl")
            with open(filepath, 'w') as f:
                for key, value in data.items():
                    json_line = json.dumps({key: value})
                    f.write(json_line + '\n')
        
        else:
            raise ValueError(f"Unsupported mode: {self.mode}")
        

class txtDataSink(DataSink):
    '''
        Data sink for saving data to txt files
    '''
    def __init__(self, dirpath: str):
        self.dirpath = dirpath
        os.makedirs(dirpath, exist_ok=True)

    def save(self, data: dict, name) -> None:
        filepath = os.path.join(self.dirpath, f"{name}.txt")
        with open(filepath, 'w', encoding='utf-8') as f:
            for key in data:
                f.write(f'{key} {data[key]}' + '\n')
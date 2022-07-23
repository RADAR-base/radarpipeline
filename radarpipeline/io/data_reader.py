from abc import ABC, abstractmethod
from typing import Dict

from radarpipeline.datalib import Data


class DataReader(ABC):
    config: Dict

    def __init__(self, config: Dict) -> None:
        self.config = config

    @abstractmethod
    def read(self) -> Data:
        pass


class SchemaReader(ABC):
    schema_dir: str
    schema_file: str

    def __init__(self, schema_dir: str) -> None:
        self.schema_dir = schema_dir

    @abstractmethod
    def is_schema_present() -> bool:
        pass

    @abstractmethod
    def get_schema(self):
        pass

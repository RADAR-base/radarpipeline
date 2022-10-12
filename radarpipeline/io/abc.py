from abc import ABC, abstractmethod
from typing import Dict

from radarpipeline.datalib import Data
from radarpipeline.datatypes.data_types import DataType


class DataReader(ABC):
    """
    Abstract class for reading the RADAR data
    """

    config: Dict

    def __init__(self, config: Dict) -> None:
        self.config = config

    @abstractmethod
    def read_data(self) -> Data:
        pass


class SchemaReader(ABC):
    """
    Abstract class for reading the RADAR data schema
    """

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


class DataWriter(ABC):
    """
    Abstract class for writing the RADAR data
    """

    features: Dict[str, DataType]
    output_dir: str

    def __init__(self, features: Dict[str, DataType], output_dir: str) -> None:
        self.features = features
        self.output_dir = output_dir

    @abstractmethod
    def write_data(self) -> None:
        pass

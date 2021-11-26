from .reader import *
from .writer import *
from abc import ABC, abstractmethod
from typing import List, Tuple, Dict
from ..datalib import Data


class DataReader(ABC):
    def __init__(self, config: Dict) -> None:
        self.config = config

    @abstractmethod
    def read(self) -> Data:
        pass
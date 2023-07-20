from abc import ABC, abstractmethod
from typing import List

from radarpipeline.datatypes.data_types import DataType


class Feature(ABC):
    name: str
    description: str
    required_input_data: List[str]

    def __init__(
        self, name: str, description: str, required_input_data: List[str]
    ) -> None:
        self.name = name
        self.description = description
        self.required_input_data = required_input_data

    def __str__(self) -> str:
        return self.name

    def get_required_data(self) -> List[str]:
        return self.required_input_data

    @abstractmethod
    def preprocess(self, data: DataType) -> DataType:
        """
        Preprocess the data for tje feature.
        If there's nothing to preprocess, please return the input
        """
        pass

    @abstractmethod
    def calculate(self, data: DataType) -> DataType:
        """
        Calculates the feature.
        """
        pass

from abc import ABC, abstractmethod
from typing import List

from radarpipeline.datalib import RadarData


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
    def preprocess(self, data: RadarData):
        """
        Calculates the feature.
        """
        pass

    @abstractmethod
    def calculate(self, data):
        """
        Calculates the feature.
        """
        pass

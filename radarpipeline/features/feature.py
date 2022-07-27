from abc import ABC, abstractmethod
from typing import Dict, List, Tuple

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

    def get_required_data(self):
        return self.required_input_data

    @abstractmethod
    def calculate(self, data: RadarData):
        """
        Calculates the feature.
        """
        pass

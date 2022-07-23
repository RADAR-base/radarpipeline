from abc import ABC, abstractmethod
from typing import List, Tuple

from radarpipeline.datalib import RadarData
from radarpipeline.features.feature import Feature


class FeatureGroup(ABC):
    """
    A class to hold a group of features.
    """

    name: str
    description: str
    features: List[Feature]
    required_input_data: List[str]

    def __init__(self, name: str, description: str, features: List[Feature]):
        self.name = name
        self.description = description
        self.features = features
        self.required_input_data = self._compute_required_data()

    def __str__(self):
        return self.name

    def get_required_data(self):
        return self.required_input_data

    def _compute_required_data(self) -> List[str]:
        """
        Compute the required data for each feature in the group.
        """

        required_input_data = set()
        for feature in self.features:
            required_input_data.update(feature.get_required_data())
        return list(required_input_data)

    @abstractmethod
    def preprocess(self, data: RadarData) -> RadarData:
        """
        Preprocess the data for each feature in the group.
        """

        pass

    def get_all_features(self, data: RadarData):
        """
        Compute the features for each feature in the group.
        """

        feature_names = []
        feature_values = []
        for feature in self.features:
            feature_names.append(feature.name)
            feature_values.append(feature.calculate(data))
        return feature_names, feature_values

    @abstractmethod
    def compute_features(self, data: RadarData) -> RadarData:
        """
        compute and combine the features for each feature in the group.
        """

        pass

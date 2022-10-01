from abc import ABC, abstractmethod
from typing import List, Tuple

from radarpipeline.datalib import RadarData
from radarpipeline.datatypes import DataType
from radarpipeline.features.feature import Feature


class FeatureGroup(ABC):
    """
    A class to hold a group of features.
    """

    name: str
    description: str
    features: List["Feature"]
    required_input_data: List[str]

    def __init__(self, name: str, description: str, features: List["Feature"]):
        self.name = name
        self.description = description
        self.features = [f() for f in features]
        self.required_input_data = self._compute_required_data()

    def __str__(self):
        return self.name

    def get_required_data(self) -> List[str]:
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
    def preprocess(self, data: RadarData) -> DataType:
        """
        Preprocess the data for each feature in the group.
        If there's nothing to process, please return the input
        """
        pass

    def get_all_features(self, data: RadarData) -> Tuple[List[str], List[DataType]]:
        """
        Compute the features for each feature in the group.
        """

        feature_names = []
        feature_values = []
        preprocessed_data = self.preprocess(data)
        for feature in self.features:
            feature_names.append(feature.name)
            preprocessed_feature = feature.preprocess(preprocessed_data)
            feature_values.append(feature.calculate(preprocessed_feature))
        return feature_names, feature_values

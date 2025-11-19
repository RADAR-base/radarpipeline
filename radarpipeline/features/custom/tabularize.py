import pandas as pd
from radarpipeline.datalib import RadarData
from radarpipeline.features import Feature, FeatureGroup
from typing import List, Tuple
from functools import partial


class Tabularize(FeatureGroup):
    def __init__(self, variables: str | List[str]):
        name = "Tabularize"
        description = "custom feature to tabularize the data"
        feature_instance = partial(TabularizeFeature, variables)
        features = [feature_instance]
        super().__init__(name, description, features)
        self.is_custom = True
        if isinstance(variables, str):
            self.required_input_data = [variables]
        self.required_input_data = variables

    def preprocess(self, data: RadarData) -> RadarData:
        """
        Preprocess the data for each feature in the group.
        """
        return data


class TabularizeFeature(Feature):
    def __init__(self, required_input_data):
        self.name = "TabularizeFeatures"
        self.description = "Tabularize multiple variables"
        self.required_input_data = required_input_data

    def preprocess(self, data: RadarData) -> RadarData:
        """
        Preprocess the data for each feature in the group.
        """
        return data

    def calculate(self, data) -> dict:
        tabular_dict = {}
        for key in self.required_input_data:
            tabular_dict[key] = data.get_combined_data_by_variable(
                key)
        return tabular_dict

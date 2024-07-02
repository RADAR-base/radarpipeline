from abc import ABC, abstractmethod
from typing import List, Tuple
import radarpipeline


class FeatureLoader():
    """
    A  custom class to load features and return dataframe
    """

    def __init__(self, config) -> None:
        self.config = self.modify_config(config)
        print(config)
        self.project = radarpipeline.Project(config)

    def modify_config(self, config):
        """
        Modify the input configuration to include the variables of interest
        """
        if "project" not in config:
            config["project"] = {
                "project_name": "custom",
                "description": "custom",
                "version": "custom"
            }
        if "input" not in config:
            raise ValueError("Input configuration is missing")
        if "source_type" not in config["input"]:
            config["input"]["source_type"] = "local"
        if "data_format" not in config["input"]:
            config["input"]["data_format"] = "csv"
        if "output" not in config:
            config["output"] = {
                "output_location": "dataframe",
                "config": {},
                "data_format": "csv",
                "compress": False
            }
        if "configurations" not in config:
            config['configurations'] = {}
            config['configurations']['df_type'] = "pandas"
        return config

    def load_features(self):
        """
        Load the features from the source path and return the data
        """
        self.project.fetch_data()
        self.project.compute_features()
        return self.project.features

from typing import List, Union
import os
import numpy as np
from dask import dataframe as dd
import yaml
from ..features import Feature, FeatureGroup
from ..io import SFTPDataReaderCSV, LocalDataReaderCSV
import importlib
import inspect


class Project():

    # Init takes yaml or dict as an input_data
    def __init__(self, input_data: Union[str, dict]):
        self.FEATURE_PATH = os.path.join("radarpipeline/features/features")
        self.input_data = input_data
        self.config = self._get_config()
        self._validate_config()
        self.feature_groups = self._look_up_features()
        self.total_required_data = self._get_total_required_data()
        self.features = {}

    def _get_config(self):
        # Read the yaml file
        if isinstance(self.input_data, str):
            # Raise error if the file does not exist, or is empty
            if not os.path.exists(self.input_data):
                raise ValueError("Input file does not exist ")
            if os.stat(self.input_data).st_size == 0:
                raise ValueError("Input file is empty")

            with open(self.input_data, "r") as f:
                config = yaml.load(f, Loader=yaml.FullLoader)
        elif isinstance(self.input_data, dict):
            config = self.input_data
        else:
            raise ValueError(
                "Wrong input data type. Should be yaml file path or dict.")
        return config

    def _get_feature_groups_from_filepath(self, filepath):
        # convert path to python import module
        feature_name = filepath.replace("/", ".")
        feature_name = feature_name.replace(".py", "")
        feature_name = feature_name.replace("\\", ".")
        # import the feature
        feature_module = importlib.import_module(feature_name)
        # get the feature class
        feature_classes = []
        for name, obj in inspect.getmembers(feature_module):
            if inspect.isclass(obj) and obj != Feature and obj != FeatureGroup:
                if isinstance(obj(), FeatureGroup):
                    feature_classes.append(obj())
        return feature_classes

    def _look_up_feature(self, feature_name: str) -> List[FeatureGroup]:
        """
        Look up the feature group
        """
        # check if feature name is a path
        if os.path.exists(feature_name):
            feature_classes = self._get_feature_groups_from_filepath(
                feature_name)
        else:

            # iterate over the  features directory and list all the features classes
            # If all_feature_classes does not exists then create it
            if not hasattr(self, "all_feature_classes"):
                for root, dirs, files in os.walk(self.FEATURE_PATH):
                    for file in files:
                        if file.endswith(".py"):
                            self.all_feature_classes = self._get_feature_groups_from_filepath(
                                os.path.join(root, file))
            # search feature_name in all_feature_classes
            for feature_class in self.all_feature_classes:
                if feature_class.name == feature_name:
                    return [feature_class]
            raise ValueError(f"Feature {feature_name} not found")
        return feature_classes

    def _look_up_features(self,) -> List[FeatureGroup]:
        """
        Look up the features group
        """
        feature_names = self.config.get("features")
        feature_groups = []
        for feature_name in feature_names:
            feature_groups += self._look_up_feature(feature_name)
        return feature_groups

    def _get_total_required_data(self) -> List[str]:
        """
        Get the total required data
        """
        total_required_data = set()
        for feature_group in self.feature_groups:
            total_required_data.update(feature_group.get_required_data())
        return list(total_required_data)

    def _validate_config(self):
        # Check if all the required keys are present
        required_keys = ["input_data", "project", "features", "output_data"]
        for key in required_keys:
            if key not in self.config:
                raise ValueError(f"{key} is not present in the config file")

        #  check if input_data satisfies all the conditions
        if self.config["input_data"]["data_location"] == "sftp":
            sftp_config_keys = [
                "sftp_host",
                "sftp_username",
                "sftp_directory",
                "sftp_private_key"
            ]
            for key in sftp_config_keys:
                if key not in self.config["input_data"]:
                    raise ValueError(
                        f"{key} is not present in the config file")

        elif self.config["input_data"]["data_location"] == "local":
            if "local_directory" not in self.config["input_data"]:
                raise ValueError(
                    "local_directory is not present in the config file")
        else:
            raise ValueError("data_location is not present in the config file")

        # Check if output_data satisfies all the conditions
        if self.config["output_data"]["output_location"] == "postgres":
            postgres_config_keys = [
                "postgres_host",
                "postgres_username",
                "postgres_database",
                "postgres_password",
                "postgres_table"
            ]
            for key in postgres_config_keys:
                if key not in self.config["output_data"]:
                    raise ValueError(
                        f"{key} is not present in the config file")

        elif self.config["output_data"]["output_location"] == "local":
            if "output_directory" not in self.config["output_data"]:
                raise ValueError(
                    "output_directory is not present in the config file")

            # Raise error if output_format it not csv or xlsx
            if self.config["output_data"]["output_format"] not in ["csv", "xlsx"]:
                raise ValueError("Wrong output_format")
        else:
            raise ValueError(
                "output_location is not present in the config file")

        # Check features array atleast has one element
        if len(self.config["features"]) == 0:
            raise ValueError("features array is empty")

    def fetch_data(self):
        if self.config["input_data"]["data_location"] == "sftp":
            if self.config["input_data"]["data_format"] == "csv":
                self.data = SFTPDataReaderCSV(
                    self.config["input_data"], self.total_required_data).read()
            else:
                raise ValueError("Wrong data_format")
        elif self.config["input_data"]["data_location"] == "local":
            if self.config["input_data"]["data_format"] == "csv":
                self.data = LocalDataReaderCSV(
                    self.config["input_data"], self.total_required_data).read()
            else:
                raise ValueError("Wrong data_format")
        else:
            raise ValueError("Wrong data_location")

    def compute_features(self):
        for feature_group in self.feature_groups:
            self.features[feature_group] = feature_group.compute_features(
                self.data)

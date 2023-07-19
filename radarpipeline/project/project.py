import importlib
import inspect
import logging
import os
import pathlib
import sys
from typing import Any, Dict, List, Union

from git.exc import GitCommandError
from git.repo import Repo

from radarpipeline.common import utils
from radarpipeline.features import Feature, FeatureGroup
from radarpipeline.io import PandasDataWriter, SparkCSVDataReader, SparkDataWriter
from radarpipeline.io import SftpDataReader
from radarpipeline.project.validations import ConfigValidator
from strictyaml import load, YAMLError

logger = logging.getLogger(__name__)


class Project:
    def __init__(self, input_data: Union[str, dict]) -> None:
        """
        Initialize the project with the data from the config file, or a config dict

        Parameters
        ---------
        input_data: Union[str, dict]
            Path to the config file or a dict containing the config
        """

        self.valid_input_formats = ["csv", "csv.gz"]
        self.valid_output_formats = ["csv"]
        self.input_data = self._resolve_input_data(input_data)
        self.feature_path = os.path.abspath(
            os.path.join("radarpipeline", "features", "features")
        )
        self.features = {}
        self.config = self._get_config()
        self.validator = ConfigValidator(self.config, self.valid_input_formats,
                                         self.valid_output_formats)
        self.validator.validate()
        self.feature_groups = self._get_feature_groups()
        self.total_required_data = self._get_total_required_data()

    def _resolve_input_data(self, input_data) -> str:
        """
        Resolves the input_data variable and check
        if it is a local path or a remote github url.
        Returns:
            str: Path to the config file
        """
        # checking if input_data is a local path
        if isinstance(input_data, str):
            if os.path.exists(input_data):
                return input_data
            elif utils.is_valid_github_path(input_data):
                # get config.yaml from github
                try:
                    repo_name = utils.get_repo_name_from_url(input_data)
                    cache_dir = os.path.join(
                        os.path.expanduser("~"), ".cache", "radarpipeline", repo_name
                    )
                    if not os.path.exists(cache_dir):
                        Repo.clone_from(input_data, cache_dir)
                    input_data = os.path.join(cache_dir, "config.yaml")
                    return input_data
                except GitCommandError as err:
                    logger.error(f"Error while cloning the repo: {err}")
                    sys.exit(1)
            else:
                raise ValueError(f"File or URL does not exist: {input_data}")
        return input_data

    def _get_config(self) -> Dict[str, Any]:
        """
        Reads the config and returns it as a dictionary

        Returns
        -------
        Dict[str, Any]
            Config dictionary
        """

        if isinstance(self.input_data, str):
            try:
                config = utils.read_yaml(self.input_data)
            except YAMLError as err:
                logger.error(f"Error while reading config file: {err}")
                sys.exit(1)
        elif isinstance(self.input_data, dict):
            config = self.input_data
        else:
            raise ValueError("Wrong input data type. Should be yaml file path or dict.")

        logger.info("Config file read successfully")
        return config

    def _get_feature_groups(self) -> List[FeatureGroup]:
        """
        Get all the feature groups from the config

        Returns
        -------
        List[FeatureGroup]
            List of all the feature groups
        """

        features = self.config.get("features", [])
        feature_groups = set()

        if "mock" in features:
            logger.info("Using mock features")
        else:
            for feature in features:
                feature_groups.update(self._get_feature_group(feature))
            feature_group_names = [
                feature_group.name for feature_group in feature_groups
            ]
            logger.info(f"Number of feature groups found: {len(list(feature_groups))}")
            logger.info(f"List of Feature groups: {feature_group_names}")

        return list(feature_groups)

    def _get_feature_group(self, feature: dict) -> List[FeatureGroup]:
        """
        Get feature group from filepath or name

        Parameters
        ----------
        feature: dict
            Feature to get from the specified location

        Returns
        -------
        List[FeatureGroup]
            List of feature group(s)
        """

        feature_group_classes = set()

        feature_location = feature["location"]
        req_feature_groups = feature["feature_groups"]

        # Get feature class from __init__.py file in feature_location
        all_feature_group_classes = self._get_feature_groups_from_filepath(
            feature_location
        )

        # Search feature_name in all_feature_classes
        for feature_group_class in all_feature_group_classes:
            if feature_group_class.name in req_feature_groups:
                feature_group_classes.add(feature_group_class)

        if len(feature_group_classes) == 0:
            raise ValueError(f"Feature not found: {feature}")

        return list(feature_group_classes)

    def _get_feature_groups_from_filepath(self, feature_location: str) -> List[Any]:
        """
        Gets the feature group classes from the filepath

        Parameters
        ----------
        feature_location: str
            Filepath of the feature group classes

        Returns
        -------
        List[Any]
            List of feature group classes
        """

        parent_dir = os.path.dirname(feature_location)
        base_name = os.path.basename(feature_location)

        sys.path.insert(1, parent_dir)

        # Import the feature module
        feature_module = importlib.import_module(base_name)

        # Get the feature group class
        feature_groups = set()
        for name, obj in inspect.getmembers(feature_module, inspect.isclass):
            if obj != Feature and obj != FeatureGroup:
                instance = obj()
                if isinstance(instance, FeatureGroup):
                    feature_groups.add(instance)

        return list(feature_groups)

    def _get_total_required_data(self) -> List[str]:
        """
        Get the total required data fromm the feature groups

        Returns
        -------
        List[str]
            List of all the required data
        """
        self.computable_feature_names = self.config["features"][0]['feature_names']
        total_required_data = set()
        for i, feature_group in enumerate(self.feature_groups):
            if self.computable_feature_names[i][0] == 'all':
                total_required_data.update(feature_group.get_required_data())
            else:
                total_required_data.update(
                    feature_group.get_listed_required_data(
                        self.computable_feature_names[i]
                    )
                )
        logger.info(f"Total required data: {total_required_data}")
        return list(total_required_data)

    def fetch_data(self) -> None:
        """
        Fetches the data from the data source
        """
        if 'spark_config' not in self.config:
            self.config['spark_config'] = {}

        if self.config["input"]["data_type"] == "local":
            if self.config["input"]["data_format"] in self.valid_input_formats:
                sparkcsvdatareader = SparkCSVDataReader(
                    self.config["input"],
                    self.total_required_data,
                    self.config["configurations"]["df_type"],
                    self.config['spark_config']
                )
                self.data = sparkcsvdatareader.read_data()
                sparkcsvdatareader.close_spark_session()
            else:
                raise ValueError("Wrong data format")

        elif self.config["input"]["data_type"] == "mock":
            MOCK_URL = "https://github.com/RADAR-base-Analytics/mockdata"
            cache_dir = os.path.join(
                os.path.expanduser("~"), ".cache", "radarpipeline", "mockdata")
            if not os.path.exists(cache_dir):
                Repo.clone_from(MOCK_URL, cache_dir)
            mock_data_directory = os.path.join(cache_dir, "mockdata")
            mock_config_input = {
                "config": {
                    "source_path": mock_data_directory
                }
            }
            sparkcsvdatareader = SparkCSVDataReader(
                mock_config_input, self.total_required_data,
                spark_config=self.config['spark_config']
            )
            self.data = sparkcsvdatareader.read_data()
            sparkcsvdatareader.close_spark_session()

        elif self.config["input"]["data_type"] == "sftp":
            sftp_data_reader = SftpDataReader(self.config["input"]["config"],
                                              self.total_required_data)
            root_dir = sftp_data_reader.get_root_dir()
            logger.info("Reading data from sftp")
            sftp_data_reader.read_sftp_data()
            sftp_local_config = {
                "config": {
                    "source_path": root_dir
                }
            }
            sparkcsvdatareader = SparkCSVDataReader(
                sftp_local_config,
                self.total_required_data,
                self.config["configurations"]["df_type"],
                self.config['spark_config']
            ).read_data()
            self.data = sparkcsvdatareader.read_data()
            sparkcsvdatareader.close_spark_session()
        else:
            raise ValueError("Wrong data location")

    def compute_features(self) -> None:
        """
        Computes the features from the ingested data
        """
        self.computable_feature_names = self.config[
            "features"][0]['feature_names']
        for i, feature_group in enumerate(self.feature_groups):
            if self.computable_feature_names[i][0] == "all":
                feature_names, feature_values = feature_group.get_all_features(
                    self.data
                )
            else:
                feature_names, feature_values = feature_group.get_listed_features(
                    self.computable_feature_names[i], self.data,
                )
            for feature_name, feature_value in zip(feature_names, feature_values):
                self.features[feature_name] = feature_value

    def export_data(self) -> None:
        """
        Exports the computed features to the specified location
        """

        if self.config["configurations"]["df_type"] == "pandas":
            writer = PandasDataWriter(
                self.features,
                self.config["output"]['config']["target_path"],
                self.config["output"]["compress"],
            )
        elif self.config["configurations"]["df_type"] == "spark":
            writer = SparkDataWriter(
                self.features,
                self.config["output"]['config']["target_path"],
                self.config["output"]["compress"],
            )
        else:
            raise ValueError("Wrong df_type")
        writer.write_data()

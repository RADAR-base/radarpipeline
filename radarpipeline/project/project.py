import importlib
import inspect
import logging
import os
import pathlib
from typing import Any, Dict, List, Union

from git.repo import Repo

from radarpipeline.common import utils
from radarpipeline.features import Feature, FeatureGroup
from radarpipeline.io import SparkCSVDataReader

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

        self.feature_path = os.path.abspath(
            os.path.join("radarpipeline", "features", "features")
        )
        self.input_data = input_data
        self.config = self._get_config()
        self._validate_config()
        self.feature_groups = self._get_feature_groups()
        self.total_required_data = self._get_total_required_data()
        self.features = {}

    def _get_config(self) -> Dict[str, Any]:
        """
        Reads the config and returns it as a dictionary

        Returns
        -------
        Dict[str, Any]
            Config dictionary
        """

        if isinstance(self.input_data, str):
            config = utils.read_yaml(self.input_data)
        elif isinstance(self.input_data, dict):
            config = self.input_data
        else:
            raise ValueError("Wrong input data type. Should be yaml file path or dict.")

        logger.info("Config file read successfully")
        return config

    def _validate_config(self) -> None:
        """
        Validates the config to check if all the keys are present
        """

        required_keys = ["project", "input_data", "features", "output_data"]
        for key in required_keys:
            if key not in self.config:
                raise ValueError(f"Key not present in the config: {key}")

        self._validate_input()
        self._validate_features()
        self._validate_output()

        logger.info("Config file validated successfully")

    def _validate_input(self) -> None:
        """
        Validates the input data config
        """

        if self.config["input_data"]["data_location"] == "sftp":
            sftp_config_keys = [
                "sftp_host",
                "sftp_username",
                "sftp_directory",
                "sftp_private_key",
            ]
            for key in sftp_config_keys:
                if key not in self.config["input_data"]:
                    raise ValueError(f"Key not present in the config: {key}")

        elif self.config["input_data"]["data_location"] == "local":
            if "local_directory" not in self.config["input_data"]:
                raise ValueError("Key not present in the config: local_directory")
            else:
                # Check if local_directory is absolute path. If not, then set it.
                local_directory = self.config["input_data"]["local_directory"]
                local_directory = self._get_absolute_path(local_directory)
                if not os.path.exists(local_directory):
                    raise ValueError(f"Path does not exist: {local_directory}")
                self.config["input_data"]["local_directory"] = local_directory

        elif self.config["input_data"]["data_location"] == "mock":
            if "data_format" not in self.config["input_data"]:
                raise ValueError("Key not present in the config file: data_format")
            self._update_mock_data()

        else:
            raise ValueError("Invalid value for the key: data_location")

    def _validate_features(self) -> None:
        """
        Validates the features config
        """

        if len(self.config["features"]) == 0:
            raise ValueError("features array cannot be empty")

        for index, feature in enumerate(self.config["features"]):
            if "location" not in feature:
                raise ValueError(
                    f"Key not present in the config: location at index {index}"
                )

            if "feature_group" not in feature:
                raise ValueError(
                    f"Key not present in the config: feature_group at index {index}"
                )

            if len(feature["feature_group"]) == 0:
                raise ValueError(
                    f"feature_group array cannot be empty at index {index}"
                )

            feature_location = feature["location"]

            if utils.is_valid_github_path(feature_location):
                repo_name = utils.get_repo_name_from_url(feature_location)
                cache_dir = os.path.join(
                    os.path.expanduser("~"), ".cache", "radarpipeline", repo_name
                )

                if os.path.exists(cache_dir):
                    repo = Repo(cache_dir)
                    repo.git.reset("--hard")
                    repo.git.clean("-xdf")
                    repo.remotes.origin.pull()
                else:
                    Repo.clone_from(feature_location, cache_dir)

                feature_location = cache_dir
                logger.info(f"Using feature from cache: {feature_location}")
            else:
                feature_location = os.path.expanduser(feature_location)
                if not os.path.isdir(feature_location):
                    raise ValueError(f"Invalid feature location: {feature_location}")
                logger.info(f"Using feature from local path: {feature_location}")

            self.config["features"][index]["location"] = feature_location

    def _validate_output(self) -> None:
        """
        Validates the output data config
        """

        if self.config["output_data"]["output_location"] == "postgres":
            postgres_config_keys = [
                "postgres_host",
                "postgres_username",
                "postgres_database",
                "postgres_password",
                "postgres_table",
            ]
            for key in postgres_config_keys:
                if key not in self.config["output_data"]:
                    raise ValueError(f"Key not present in the config: {key}")

        elif self.config["output_data"]["output_location"] == "local":
            if "output_directory" not in self.config["output_data"]:
                raise ValueError("Key not present in the config: output_directory")
            else:
                # Check if output_directory is absolute path. If not, then set it.
                output_directory = self.config["output_data"]["output_directory"]
                output_directory = self._get_absolute_path(output_directory)
                if not os.path.exists(output_directory):
                    raise ValueError(f"Path does not exist: {output_directory}")
                self.config["output_data"]["output_directory"] = output_directory

            # Raise error if output_format it not csv or xlsx
            if self.config["output_data"]["output_format"] not in ["csv", "xlsx"]:
                raise ValueError(
                    "Invalid value for key: output_format\nHas to be csv or xlsx"
                )

        elif self.config["output_data"]["output_location"] == "mock":
            pass

        else:
            raise ValueError("Key not present in the config: output_location")

    def _get_absolute_path(self, path: str) -> str:
        """
        Returns the absolute path of the path

        Parameters
        ----------
        path: str
            Path to be converted to absolute path

        Returns
        -------
        str
            Absolute path of the path
        """

        if not os.path.isabs(path):
            pipeline_dir = pathlib.Path(__file__).parent.parent.parent.resolve()
            path = os.path.join(pipeline_dir, path)
        return path

    def _update_mock_data(self) -> None:
        """
        Updates the mock data submodule of the project
        """

        repo = Repo(
            os.path.dirname(os.path.abspath(__file__)),
            search_parent_directories=True,
        )
        sms = repo.submodules
        try:
            mock_data_submodule = sms["mock-data"]
        except IndexError:
            raise ValueError("mock-data submodule not found in the repository")
        if not (mock_data_submodule.exists() and mock_data_submodule.module_exists()):
            logger.info("Mock data submodule not found. Cloning it...")
            repo.git.submodule("update", "--init", "--recursive")
            logger.info("Mock data input cloned")

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
                feature_groups.add(self._get_feature_group(feature))
            logger.info(f"Number of feature groups found: {len(feature_groups)}")
            logger.debug(f"List of Feature groups: {feature_groups}")

        return list(feature_groups)

    def _get_feature_group(self, feature_name: dict) -> List[FeatureGroup]:
        """
        Get feature group from filepath or name

        Parameters
        ----------
        feature_name: dict
            Feature to get from the specified location

        Returns
        -------
        List[FeatureGroup]
            List of feature group(s)
        """

        feature_group_classes = []

        # Check if feature_name is a valid feature group path
        absolute_feature_path = self._get_absolute_path(feature_name)
        if os.path.exists(absolute_feature_path):
            feature_group_classes = self._get_feature_groups_from_filepath(
                absolute_feature_path
            )
        else:
            # Iterate over the features directory and list all the features classes
            # If all_feature_classes does not exists then create it
            if not hasattr(self, "all_feature_classes"):
                for root, dirs, files in os.walk(self.feature_path):
                    for file in files:
                        if file.endswith(".py"):
                            self.all_feature_classes = (
                                self._get_feature_groups_from_filepath(
                                    os.path.join(root, file)
                                )
                            )
            # Search feature_name in all_feature_classes
            for feature_class in self.all_feature_classes:
                if feature_class.name == feature_name:
                    feature_group_classes.append(feature_class)

        if len(feature_group_classes) == 0:
            raise ValueError(f"Feature not found: {feature_name}")

        return feature_group_classes

    def _get_feature_groups_from_filepath(self, filepath: str) -> List[Any]:
        """
        Gets the feature group classes from the filepath

        Parameters
        ----------
        filepath: str
            Filepath of the feature group class

        Returns
        -------
        List[Any]
            List of feature group classes
        """

        # Convert filepath to module path
        filepath_without_extension = filepath.replace(".py", "")
        feature_module_name = ".".join(filepath_without_extension.split(os.sep))

        # Import the feature module
        feature_module = importlib.import_module(feature_module_name)

        # Get the feature group class
        feature_groups = []
        for name, obj in inspect.getmembers(feature_module):
            if inspect.isclass(obj) and obj != Feature and obj != FeatureGroup:
                if isinstance(obj(), FeatureGroup):
                    feature_groups.append(obj())

        return feature_groups

    def _get_total_required_data(self) -> List[str]:
        """
        Get the total required data fromm the feature groups

        Returns
        -------
        List[str]
            List of all the required data
        """

        total_required_data = set()
        for feature_group in self.feature_groups:
            total_required_data.update(feature_group.get_required_data())

        return list(total_required_data)

    def fetch_data(self) -> None:
        """
        Fetches the data from the data source
        """

        if self.config["input_data"]["data_location"] == "local":
            if self.config["input_data"]["data_format"] == "csv":
                self.data = SparkCSVDataReader(
                    self.config["input_data"], self.total_required_data
                ).read()
                pass
            else:
                raise ValueError("Wrong data format")

        elif self.config["input_data"]["data_location"] == "mock":
            if self.config["input_data"]["data_format"] == "csv":
                mock_config = {
                    "local_directory": os.path.join("mock-data", "mock-data")
                }
                mock_required_data = [
                    "android_phone_battery_level",
                    "android_phone_step_count",
                ]
                self.data = SparkCSVDataReader(mock_config, mock_required_data).read()
            else:
                raise ValueError("Wrong data format")

        else:
            raise ValueError("Wrong data location")

    def compute_features(self) -> None:
        """
        Computes the features from the ingested data
        """

        for feature_group in self.feature_groups:
            self.features[feature_group] = feature_group.compute_features(self.data)

import importlib
import inspect
import logging
import os
import pathlib
import sys
from typing import Any, Dict, List, Union

from git.repo import Repo
from git.exc import GitCommandError

from radarpipeline.common import utils
from radarpipeline.features import Feature, FeatureGroup
from radarpipeline.io import PandasDataWriter, SparkCSVDataReader, SparkDataWriter

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
        self.input_data = input_data
        self.feature_path = os.path.abspath(
            os.path.join("radarpipeline", "features", "features")
        )
        self.features = {}
        self.config = self._get_config()
        self._validate_config()
        self.feature_groups = self._get_feature_groups()
        self.total_required_data = self._get_total_required_data()

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
        self._validate_configurations()
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

            # Raise error if data_format it not valid input formats
            if self.config["input_data"]["data_format"] not in self.valid_input_formats:
                raise ValueError("Invalid value for key in input_data: data_format")

        elif self.config["input_data"]["data_location"] == "mock":
            self._update_mock_data()

        else:
            raise ValueError("Invalid value for the key: data_location")

    def _validate_configurations(self) -> None:
        """
        Validates the configurations config
        """

        if "configurations" not in self.config:
            self.config["configurations"] = {}

        valid_df_types = ["pandas", "spark"]

        if "df_type" not in self.config["configurations"]:
            self.config["configurations"]["df_type"] = "pandas"
        elif self.config["configurations"]["df_type"] not in valid_df_types:
            raise ValueError("Invalid value for the key: df_type")

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

            if "feature_groups" not in feature:
                raise ValueError(
                    f"Key not present in the config: feature_groups at index {index}"
                )

            if len(feature["feature_groups"]) == 0:
                raise ValueError(
                    f"feature_groups array cannot be empty at index {index}"
                )

            feature_location = feature["location"]

            if "branch" in feature:
                feature_branch = feature["branch"]
            else:
                feature_branch = "main"

            if utils.is_valid_github_path(feature_location):
                repo_name = utils.get_repo_name_from_url(feature_location)
                cache_dir = os.path.join(
                    os.path.expanduser("~"), ".cache", "radarpipeline", repo_name
                )

                if not os.path.exists(cache_dir):
                    Repo.clone_from(feature_location, cache_dir)

                repo = Repo(cache_dir)
                repo.git.reset("--hard")
                repo.git.clean("-xdf")
                try:
                    repo.git.checkout(feature_branch)
                except GitCommandError:
                    logger.warning(f"Branch {feature_branch} does not exist. Using Main branch instead.")
                    feature_branch = repo.active_branch.name
                    repo.git.checkout(feature_branch)
                repo.remotes.origin.pull(feature_branch)

                feature_location = cache_dir
                logger.info(f"Using feature from cache location: {feature_location}")
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
            if "local_directory" not in self.config["output_data"]:
                raise ValueError("Key not present in the config: local_directory")
            else:
                # Check if local_directory is absolute path. If not, then set it.
                local_directory = self.config["output_data"]["local_directory"]
                local_directory = self._get_absolute_path(local_directory)
                if not os.path.exists(local_directory):
                    os.makedirs(local_directory, exist_ok=True)
                self.config["output_data"]["output_directory"] = local_directory

            # Raise error if data_format it not valid output formats
            if (
                self.config["output_data"]["data_format"]
                not in self.valid_output_formats
            ):
                raise ValueError("Invalid value for key in output_data: data_format")

            if "compress" not in self.config["output_data"]:
                self.config["output_data"]["compress"] = False
            if self.config["output_data"]["compress"] == "true":
                self.config["output_data"]["compress"] = True

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
            mock_data_submodule = sms["mockdata"]
        except IndexError:
            raise ValueError("mockdata submodule not found in the repository")
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

        total_required_data = set()
        for feature_group in self.feature_groups:
            total_required_data.update(feature_group.get_required_data())

        logger.info(f"Total required data: {total_required_data}")

        return list(total_required_data)

    def fetch_data(self) -> None:
        """
        Fetches the data from the data source
        """

        if self.config["input_data"]["data_location"] == "local":
            if self.config["input_data"]["data_format"] in self.valid_input_formats:
                self.data = SparkCSVDataReader(
                    self.config["input_data"],
                    self.total_required_data,
                    self.config["configurations"]["df_type"],
                ).read_data()
            else:
                raise ValueError("Wrong data format")

        elif self.config["input_data"]["data_location"] == "mock":
            mock_config_input = {
                "local_directory": os.path.join("mockdata", "mockdata")
            }
            mock_required_data = [
                "android_phone_battery_level",
                "android_phone_step_count",
            ]
            self.data = SparkCSVDataReader(
                mock_config_input, mock_required_data
            ).read_data()

        else:
            raise ValueError("Wrong data location")

    def compute_features(self) -> None:
        """
        Computes the features from the ingested data
        """

        for feature_group in self.feature_groups:
            feature_names, feature_values = feature_group.get_all_features(self.data)
            for feature_name, feature_value in zip(feature_names, feature_values):
                self.features[feature_name] = feature_value

    def export_data(self) -> None:
        """
        Exports the computed features to the specified location
        """

        if self.config["configurations"]["df_type"] == "pandas":
            writer = PandasDataWriter(
                self.features,
                self.config["output_data"]["local_directory"],
                self.config["output_data"]["compress"],
            )
        elif self.config["configurations"]["df_type"] == "spark":
            writer = SparkDataWriter(
                self.features,
                self.config["output_data"]["local_directory"],
                self.config["output_data"]["compress"],
            )
        else:
            raise ValueError("Wrong df_type")
        writer.write_data()

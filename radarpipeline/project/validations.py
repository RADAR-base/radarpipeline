import os
import logging
from git.exc import GitCommandError
from git.repo import Repo

from radarpipeline.common import utils

logger = logging.getLogger(__name__)


class ConfigValidator():
    def __init__(self, config, valid_input_formats, valid_output_formats) -> None:
        self.valid_input_formats = valid_input_formats
        self.valid_output_formats = valid_output_formats
        self.config = config

    def validate(self):
        """
        Validates the config to check if all the keys are present
        """

        required_keys = ["project", "input", "features", "output"]
        for key in required_keys:
            if key not in self.config:
                raise ValueError(f"Key not present in the config: {key}")

        self._validate_input()
        self._validate_configurations()
        self._validate_features()
        self._validate_output()

        logger.info("Config file validated successfully")

    def _update_mock_data(self) -> None:
        """
        Updates the mock data submodule of the åproject
        """

        try:
            MOCK_URL = "https://github.com/RADAR-base-Analytics/mockdata"
            cache_dir = os.path.join(
                os.path.expanduser("~"), ".cache", "radarpipeline", "mockdata")
            if not os.path.exists(cache_dir):
                logger.info("Mock data submodule not found. Cloning it...")
                Repo.clone_from(MOCK_URL, cache_dir)
                logger.info("Mock data input cloned")
            else:
                repo = Repo(
                    cache_dir,
                    search_parent_directories=True,
                )
                repo.remotes.origin.pull()

        except IndexError:
            raise ValueError("mockdata submodule not found")

    def _validate_input(self) -> None:
        """
        Validates the input data config
        """

        if self.config["input"]["data_type"] == "sftp":
            sftp_config_keys = [
                "sftp_host",
                "sftp_username",
                "sftp_source_path",
                "sftp_private_key",
            ]
            for key in sftp_config_keys:
                if key not in self.config["input"]["config"]:
                    raise ValueError(f"Key not present in the config: {key}")

        elif self.config["input"]["data_type"] == "local":
            if "source_path" not in self.config["input"]["config"]:
                raise ValueError("Key not present in the config: source_path")
            else:
                # Check if local_directory is absolute path. If not, then set it.
                local_directory = self.config["input"]["config"]["source_path"]
                if isinstance(local_directory, list):
                    local_directory = [
                        self._get_absolute_path(local_path)
                        for local_path in local_directory]
                    for local_path in local_directory:
                        if not os.path.exists(local_path):
                            raise ValueError(f"Path does not exist: {local_path}")
                else:
                    local_directory = utils.get_absolute_path(local_directory)
                    if not os.path.exists(local_directory):
                        raise ValueError(f"Path does not exist: {local_directory}")
                self.config["input"]["local_directory"] = local_directory

            # Raise error if data_format it not valid input formats
            if self.config["input"]["data_format"] not in self.valid_input_formats:
                raise ValueError("Invalid value for key in input: data_format")

        elif self.config["input"]["data_type"] == "mock":
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
            if "feature_names" not in feature:
                raise ValueError(
                    f"Key not present in the config: feature_groups at index {index}"
                )

            if len(feature["feature_names"]) == 0:
                raise ValueError(
                    f"feature_groups array cannot be empty at index {index}"
                )
            feature_location = feature["location"]

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

                active_branch_name = repo.active_branch.name
                feature_branch = feature.get("branch", active_branch_name)

                try:
                    repo.git.checkout(feature_branch)
                except GitCommandError:
                    logger.warning(
                        "Branch %s does not exist. Using the %s branch instead.",
                        feature_branch,
                        repo.active_branch.name,
                    )
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

        if self.config["output"]["output_location"] == "postgres":
            postgres_config_keys = [
                "postgres_host",
                "postgres_username",
                "postgres_database",
                "postgres_password",
                "postgres_table",
            ]
            for key in postgres_config_keys:
                if key not in self.config["output"]['config']:
                    raise ValueError(f"Key not present in the config: {key}")

        elif self.config["output"]["output_location"] == "local":
            if "target_path" not in self.config["output"]['config']:
                raise ValueError("Key not present in the config: target_path")
            else:
                # Check if local_directory is absolute path. If not, then set it.
                local_directory = self.config["output"]['config']["target_path"]
                local_directory = utils.get_absolute_path(local_directory)
                if not os.path.exists(local_directory):
                    os.makedirs(local_directory, exist_ok=True)
                self.config["output"]['config']["target_path"] = local_directory

            # Raise error if data_format it not valid output formats
            if (
                self.config["output"]["data_format"]
                not in self.valid_output_formats
            ):
                raise ValueError("Invalid value for key in output: data_format")

            if "compress" not in self.config["output"]:
                self.config["output"]["compress"] = False
            if self.config["output"]["compress"] == "true":
                self.config["output"]["compress"] = True
        else:
            raise ValueError("Key not present in the config: output_location")

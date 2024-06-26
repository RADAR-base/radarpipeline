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

        if self.config["input"]["source_type"] == "sftp":
            sftp_config_keys = [
                "sftp_host",
                "sftp_username",
                "sftp_source_path",
                "sftp_private_key",
            ]
            for key in sftp_config_keys:
                if key not in self.config["input"]["config"]:
                    raise ValueError(f"Key not present in the config: {key}")

        elif self.config["input"]["source_type"] == "local":
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

        elif self.config["input"]["source_type"] == "mock":
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
        valid_user_sampling_methods = ["fraction", "count", "userid"]

        if "df_type" not in self.config["configurations"]:
            self.config["configurations"]["df_type"] = "pandas"
        elif self.config["configurations"]["df_type"] not in valid_df_types:
            raise ValueError("Invalid value for the key: df_type")
        if "user_sampling" in self.config["configurations"]:
            if "method" not in self.config["configurations"]["user_sampling"]:
                raise ValueError("Key not present in the user_sampling config: method")
            elif (
                self.config["configurations"]["user_sampling"]["method"]
                not in valid_user_sampling_methods
            ):
                raise ValueError("Invalid value for the key: method")
            else:
                self.config["configurations"][
                    "user_sampling"] = self._validate_user_sampling_config(
                        self.config["configurations"]["user_sampling"])
        else:
            self.config["configurations"]["user_sampling"] = None

        # Validating data sampling
        valid_data_sampling_methods = ["fraction", "count", "time"]
        if "data_sampling" in self.config["configurations"]:
            if "method" not in self.config["configurations"]["data_sampling"]:
                raise ValueError("Key not present in the data_sampling config: method")
            elif (
                self.config["configurations"]["data_sampling"]["method"]
                not in valid_data_sampling_methods
            ):
                raise ValueError("Invalid value for the key: method")
            else:
                self.config["configurations"][
                    "data_sampling"] = self._validate_data_sampling_config(
                        self.config["configurations"]["data_sampling"])
        else:
            self.config["configurations"]["data_sampling"] = None

    def _validate_user_sampling_config(self, user_sampling_config):
        """
        Validates the user_sampling config
        """

        if user_sampling_config["method"] == "fraction":
            user_sampling_config = self._validate_sampling_fraction(
                user_sampling_config)
        elif user_sampling_config["method"] == "count":
            user_sampling_config = self._validate_sampling_count(
                user_sampling_config)
        elif user_sampling_config["method"] == "userid":
            user_sampling_config = self._validate_sampling_userids(
                user_sampling_config)
        return user_sampling_config

    def _validate_data_sampling_config(self, data_sampling_config):
        if data_sampling_config["method"] == "fraction":
            data_sampling_config = self._validate_sampling_fraction(
                data_sampling_config)
        elif data_sampling_config["method"] == "count":
            data_sampling_config = self._validate_sampling_count(
                data_sampling_config)
        elif data_sampling_config["method"] == "time":
            data_sampling_config = self._validate_sampling_time(
                data_sampling_config)
        return data_sampling_config

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
            if feature_location == "custom":
                pass
            elif utils.is_valid_github_path(feature_location):
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
        elif self.config["output"]["output_location"] == "dataframe":
            pass
        else:
            raise ValueError("Key not present in the config: output_location")

    def _validate_sampling_fraction(self, sampling_config):
        if "config" not in sampling_config:
            raise ValueError("Key not present in the user_sampling config: config")
        if "fraction" not in sampling_config["config"]:
            raise ValueError(
                "Key not present in the user_sampling config: fraction"
            )
        # converting fraction to float
        try:
            sampling_config["config"]["fraction"] = float(
                sampling_config["config"]["fraction"]
            )
        except ValueError:
            raise ValueError(
                "Invalid value for the key: fraction. It should be a number"
            )
        if not 0 < sampling_config["config"]["fraction"] <= 1:
            raise ValueError(
                "Invalid value for the key: fraction. It should be between 0 and 1"
            )
        return sampling_config

    def _validate_sampling_count(self, sampling_config):
        if "config" not in sampling_config:
            raise ValueError("Key not present in the user_sampling config: config")
        if "count" not in sampling_config["config"]:
            raise ValueError("Key not present in the user_sampling config: count")
        try:
            sampling_config["config"]["count"] = int(
                sampling_config["config"]["count"]
            )
        except ValueError:
            raise ValueError(
                "Invalid value for the key: count. It should be a number"
            )
        if sampling_config["config"]["count"] <= 0:
            raise ValueError(
                "Invalid value for the key: count. It should be greater than 0"
            )
        return sampling_config

    def _validate_sampling_userids(self, sampling_config):
        if "config" not in sampling_config:
            raise ValueError("Key not present in the user_sampling config: config")
        if "userids" not in sampling_config["config"]:
            raise ValueError("Key not present in the user_sampling config: userids")
        if len(sampling_config["config"]["userids"]) == 0:
            raise ValueError(
                "user_ids array cannot be empty in the user_sampling config"
            )
        # check if userids are in array. If not convert to array
        if not isinstance(sampling_config["config"]["userids"], list):
            sampling_config["config"]["userids"] = [
                sampling_config["config"]["userids"]
            ]
        return sampling_config

    def _validate_sampling_time_instance(self, time_dict: dict):
        if ("starttime" not in time_dict
            ) and (
                "endtime" not in time_dict):
            raise ValueError("Neither startime nor endtime present in the config")
        # check if starttime and endtime are can be converted into time format
        # if so, convert them
        if "starttime" in time_dict:
            time_dict['starttime'] = utils.convert_str_to_time(
                time_dict['starttime'])
        if "endtime" in time_dict:
            time_dict['endtime'] = utils.convert_str_to_time(
                time_dict['endtime'])
        if "time_column" not in time_dict:
            logger.warning("time_column not present in the config. \
                Using default time column: value.time")
            time_dict["time_column"] = "value.time"
        return time_dict

    def _validate_sampling_time(self, sampling_config):
        if "config" not in sampling_config:
            raise ValueError("Key not present in the user_sampling config: config")
        if type(sampling_config["config"]) is list:
            if len(sampling_config["config"]) == 0:
                raise ValueError(
                    "No starttime and endtime present in the config"
                )
            for i, time_dict in enumerate(sampling_config["config"]):
                sampling_config["config"][i] = self._validate_sampling_time_instance(
                    time_dict)
        else:
            sampling_config["config"] = self._validate_sampling_time_instance(
                sampling_config["config"])
        return sampling_config

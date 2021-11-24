from typing import List, Union
import os
import numpy as np
import pandas as pd
import yaml
from utils import read_yaml


class Project():

    # Init takes yaml or dict as an input_data
    def __init__(self, input_data: Union[str, dict]):
        self.input_data = input_data
        self.config = self._get_config()
        self._validate_config()

    def _get_config(self):
        # Read the yaml file
        if isinstance(self.input_data, str):
            with open(self.input_data, "r") as f:
                config = yaml.load(f, Loader=yaml.FullLoader)
        else:
            config = self.input_data
        return config

    def _validate_config(self):
        # Check if all the required keys are present
        required_keys = ["input_data", "project", "features", "output_data"]
        for key in required_keys:
            if key not in self.config:
                raise ValueError(f"{key} is not present in the config file")

        #  check if input_data is satisy all the conditions
        if self.config["input_data"]["data_location"] == "sftp":
            if "sftp_host" not in self.config["input_data"]:
                raise ValueError("sftp_host is not present in the config file")
            if "sftp_username" not in self.config["input_data"]:
                raise ValueError("sftp_username is not present in the config file")
            if "sftp_directory" not in self.config["input_data"]:
                raise ValueError("sftp_directory is not present in the config file")
            if "sftp_private_key" not in self.config["input_data"]:
                raise ValueError("sftp_private_key is not present in the config file")
        elif self.config["input_data"]["data_location"] == "local":
            if "local_directory" not in self.config["input_data"]:
                raise ValueError("local_directory is not present in the config file")
        else:
            raise ValueError("data_location is not present in the config file")

        # Check if output_data is satisy all the conditions
        if self.config["output_data"]["output_location"] == "postgres":
            if "postgres_host" not in self.config["output_data"]:
                raise ValueError("postgres_host is not present in the config file")
            if "postgres_username" not in self.config["output_data"]:
                raise ValueError("postgres_username is not present in the config file")
            if "postgres_database" not in self.config["output_data"]:
                raise ValueError("postgres_database is not present in the config file")
            if "postgres_password" not in self.config["output_data"]:
                raise ValueError("postgres_password is not present in the config file")
            if "postgres_table" not in self.config["output_data"]:
                raise ValueError("postgres_table is not present in the config file")
        elif self.config["output_data"]["output_location"] == "local":
            if "output_directory" not in self.config["output_directory"]:
                raise ValueError("local_directory is not present in the config file")
            # Raise error if output_format it not csv or xlsx
            if self.config["output_data"]["output_format"] not in ["csv", "xlsx"]:
                raise ValueError("Wrong output_format")
        else:
            raise ValueError("output_location is not present in the config file")

        # Check features array atleast has one element
        if len(self.config["features"]) == 0:
            raise ValueError("features array is empty")





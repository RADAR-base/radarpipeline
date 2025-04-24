import logging
import sys
import traceback
from typing import Dict, Union, List
from radarpipeline.project import Project
from radarpipeline.io import CustomDataReader
from radarpipeline.project.validations import ConfigGenerator
import yaml

from radarpipeline.common.logger import logger_init
import requests

logger_init()

logger = logging.getLogger(__name__)


def run(config_path: Union[str, Dict] = "config.yaml"):
    """
    Pipeline entry point.
    config_path could be a local path to a configuration file
    or a remote url to a configuration file.
    """
    try:
        logger.info("Starting the pipeline run...")
        logger.info("Reading and Validating the configuration file...")
        project = Project(input_data=config_path)
        logger.info("Fetching the data...")
        project.read_data()
        logger.info("Computing the features...")
        project.compute_features()
        logger.info("Exporting the features data...")
        project.export_data()
        logger.info("Data exported successfully. Closing Spark Engine")
        project.close_spark_session()
        logger.info("Pipeline run completed successfully")
    except KeyboardInterrupt:
        logger.info("Pipeline run interrupted by user")
        sys.exit(0)
    except Exception:
        logger.info(traceback.format_exc())
        sys.exit(1)


def validate(config: Union[str, Dict]):
    """
    Validate the configuration file.
    """
    try:
        logger.info("Validating the configuration file...")
        project = Project(input_data=config)
        project.validate()
        logger.info("Configuration file is valid")
    except Exception:
        logger.info(traceback.format_exc())
        sys.exit(1)


def generate_config(config_path: str = "./config.yaml", config_dict: Dict = None):
    """
    Generate a sample configuration file.
    """
    try:
        logger.info("Generating a sample configuration file...")
        generator = ConfigGenerator(config_dict, config_path)
        config = generator.generate_config()
        yaml.dump(config, open(config_path, "w"))
        logger.info(f"Sample configuration file generated at {config_path}")
    except Exception:
        logger.info(traceback.format_exc())
        sys.exit(1)


def read(source_path: str, variables: Union[str, List[str]]):
    """
    Read data from a source.
    """
    try:
        logger.info("Reading data...")
        input_config = {"source_path": source_path}
        data_reader = CustomDataReader(input_config, variables,
                                       source_type="local",
                                       data_format="csv",
                                       df_type="pandas")
        data = data_reader.read_data()
        logger.info("Data read successfully")
        return data
    except Exception:
        logger.info(traceback.format_exc())
        sys.exit(1)


def fetch(config: Union[Dict, str]):
    """
    Fetch data from a source.
    """
    try:
        logger.info("Fetching data...")
        project = Project(input_data=config)
        project.fetch_data()
        logger.info("Data fetched successfully")
    except Exception:
        logger.info(traceback.format_exc())
        sys.exit(1)


def convert_from_dict(config: Dict):
    """
    Convert data from one format to another.
    """
    """
    Pipeline entry point.
    config_path could be a local path to a configuration file
    or a remote url to a configuration file.
    """
    try:
        logger.info("Starting the pipeline run...")
        logger.info("Reading and Validating the configuration file...")
        project = Project(input_data=config)
        logger.info("Fetching the data...")
        project.read_data()
        logger.info("Computing the features...")
        project.compute_features()
        logger.info("Exporting the features data...")
        project.export_data()
        logger.info("Data exported successfully. Closing Spark Engine")
        project.close_spark_session()
        logger.info("Pipeline run completed successfully")
    except KeyboardInterrupt:
        logger.info("Pipeline run interrupted by user")
        sys.exit(0)
    except Exception:
        logger.info(traceback.format_exc())
        sys.exit(1)


def convert(yaml_path: str = None,
            source_path: str = None,
            destination_path: str = None,
            variables: List = None, data_format: str = "csv",
            needs_download=False, download_config=None):
    """_summary_

    Args:
        source_path (str): Path to the source file.
        destination_path (str): Path to the destination file.
        data_format (str, optional): Format of the data. Defaults to "csv".
        needs_download (bool, optional): Whether the data needs to be downloaded.
        Defaults to False.
        download_config (dict, optional): Configuration for downloading the data.
        Defaults to None.
    Reads the data from the source and converts it into a tabular format and saves it.
    """
    try:
        if yaml_path:
            run(yaml_path)
        else:
            if source_path is None:
                logger.info("Source path is required")
                sys.exit(1)
            elif destination_path is None:
                logger.info("Destination path is required")
                sys.exit(1)
            elif variables is None:
                logger.info("Variables are required")
                sys.exit(1)
            elif not isinstance(variables, list):
                if isinstance(variables, str):
                    variables = [variables]
                else:
                    logger.info("Variables should be a list")
                    sys.exit(1)
                if len(variables) == 0:
                    logger.info("Variables should not be empty")
                    sys.exit(1)
            else:
                logger.info("Reading data...")
                config_dict = _generate_tabular_config(source_path,
                                                       destination_path,
                                                       variables,
                                                       data_format,
                                                       needs_download,
                                                       download_config)
                run(config_dict)
    except Exception:
        logger.info(traceback.format_exc())
        sys.exit(1)

def _modify_config(config):
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

def compute_features(input_config: Dict,
                 feature_config: Union[Dict, List[Dict]]):
    """
    Use input configuration and compute features.
    Returns the features as a dictionary.
    Args:
        input_config (Dict): Input configuration dictionary.
        feature_config (Union[Dict, List[Dict]]): Feature configuration dictionary or list of dictionaries.
    Returns:
        Dict: Dictionary containing the computed features.
    Raises:
        Exception: If there is an error in fetching the features.
    """
    try:
        logger.info("Getting features...")
        if isinstance(feature_config, dict):
            feature_config = [feature_config]
        config = {
            "input": input_config,
            "features": feature_config
        }
        # Modify the configuration to include the variables of interest
        # and set default values for missing keys
        config = _modify_config(config)
        project = Project(input_data=config)
        project.read_data()
        project.compute_features()
        data = project.features
        logger.info("Features fetched successfully")
        return data
    except Exception:
        logger.info(traceback.format_exc())
        sys.exit(1)

def _generate_tabular_config(source_path: str,
                             destination_path: str,
                             variables: List, data_format: str,
                             needs_download, download_config):
    """
    Generate a configuration dictionary for converting data to tabular format.
    """
    config_dict = {
        "project": {
            "project_name": "tabularise",
            "description": "custom function to tabularise the data",
            "version": "0.0.0"},
        "input": {
            "source_type": "local",
            "config": {
                "source_path": source_path
            },
            "data_format": "csv"
        },
        "output": {
            "output_location": "local",
            "config": {
                "target_path": destination_path
            },
            "data_format": data_format,
        },
        "configurations": {
            "df_type": "pandas"
        }
    }
    if needs_download:
        config_dict["input"]["download"] = download_config
    config_dict["features"] = [
        {
            "location": "custom",
            "feature_groups": ["Tabularize"],
            "feature_names": [variables]
        }
    ]
    return config_dict


def _gather_clone_urls(organization, no_forks=True):
    """
    gh = pygithub3.Github()
    all_repos = gh.repos.list(user=organization).all()
    for repo in all_repos:

        # Don't print the urls for repos that are forks.
        if no_forks and repo.fork:
            continue

        yield repo.clone_url
    TODO: implement this function
    """


def show_available_pipelines():
    """
    Uses git to show the available pipelines from
    https://github.com/RADAR-base-Analytics/
    """
    try:
        logger.info(
            "Fetching available pipelines from "
            "RADAR-base-Analytics GitHub organization..."
        )
        api_url = "https://api.github.com/orgs/RADAR-base-Analytics/repos"
        response = requests.get(api_url)
        if response.status_code == 200:
            repos = response.json()
            pipelines = [
                {"url": repo["html_url"],
                 "name": repo["name"],
                 "description": repo.get("description", "No description available")}
                for repo in repos if not repo.get("fork", False)]
            logger.info(f"Found {len(pipelines)} available pipelines.")
            return pipelines
        else:
            logger.error(
                f"Failed to fetch repositories: {response.status_code} - "
                f"{response.text}"
            )
            return []
    except Exception as e:
        logger.error(f"An error occurred while fetching pipelines: {e}")
        return []


if __name__ == "__main__":
    run()
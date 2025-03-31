import logging
import sys
import traceback
from typing import Dict, Union, List
from radarpipeline.project import Project
from radarpipeline.io import CustomDataReader
from radarpipeline.project.validations import ConfigGenerator
import yaml

from radarpipeline.common.logger import logger_init

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
    """
    # Still in progress. not working yet
    clone_urls = _gather_clone_urls("RADAR-base-Analytics")
    print(clone_urls)
    return clone_urls
    # read all the repos from https://github.com/RADAR-base-Analytics/
    """


if __name__ == "__main__":
    run()

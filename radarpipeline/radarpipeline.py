import logging
import sys
import traceback

from radarpipeline import Project
from radarpipeline.common.logger import logger_init

logger_init()

logger = logging.getLogger(__name__)


def run(config_path: str = "config.yaml"):
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
        project.fetch_data()
        logger.info("Computing the features...")
        project.compute_features()
        logger.info("Exporting the features data...")
        project.export_data()
        logger.info("Pipeline run completed successfully")
    except KeyboardInterrupt:
        logger.info("Pipeline run interrupted by user")
        sys.exit(0)
    except Exception:
        logger.info(traceback.format_exc())
        sys.exit(1)


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

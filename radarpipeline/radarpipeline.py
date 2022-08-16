import logging
import sys

from radarpipeline import Project
from radarpipeline.common.logger import logger_init

logger_init()

logger = logging.getLogger(__name__)


def run():
    """
    Pipeline entry point.
    """
    try:
        logger.info("Starting the pipeline run")
        project = Project(input_data="config.yaml")
        project.fetch_data()
        project.compute_features()
        logger.info("Pipeline run completed successfully")
    except KeyboardInterrupt:
        logger.info("Pipeline run interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.info(e)
        sys.exit(1)

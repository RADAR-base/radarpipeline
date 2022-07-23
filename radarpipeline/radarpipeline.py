import logging
import sys

from radarpipeline import Project
from radarpipeline.common.logger import logger_init

logger_init()

logger = logging.getLogger(__name__)


def run():
    logger.info("Starting the pipeline run")
    project = Project(input_data="mock-config.yaml")
    project.fetch_data()
    # project.compute_features()
    logger.info("Pipeline run completed successfully")


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logger.info("Program aborted by user")
    except Exception as e:
        logger.info(e)

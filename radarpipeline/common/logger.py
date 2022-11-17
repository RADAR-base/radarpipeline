import logging
import logging.handlers
import os
import pathlib
from datetime import datetime


def logger_init(level: int = logging.INFO) -> None:
    """
    Initialize the logger

    Parameters
    ----------
    level : int, optional
        The level of the logger.
        The default is logging.INFO.
    """

    # Get logger
    logger = logging.getLogger()  # root logger
    logger.setLevel(level)

    # File handler
    path = "logs"
    year = str(datetime.now().year)
    month = str(datetime.now().month)
    filename = datetime.now().strftime("%Y%m%d") + "_logfile.log"
    log_directory = os.path.join(path, year, month)
    log_filename = os.path.join(log_directory, filename)

    pathlib.Path(log_directory).mkdir(parents=True, exist_ok=True)
    file = logging.handlers.TimedRotatingFileHandler(
        log_filename, when="midnight", interval=1
    )
    file_format = logging.Formatter("%(asctime)s [%(levelname)s]: %(message)s")
    file.setLevel(logging.INFO)
    file.setFormatter(file_format)

    # Stream handler
    stream = logging.StreamHandler()
    stream_format = logging.Formatter("%(asctime)s [%(levelname)s]: %(message)s")
    stream.setLevel(logging.INFO)
    stream.setFormatter(stream_format)

    # Adding all handlers to the logs
    logger.addHandler(file)
    logger.addHandler(stream)

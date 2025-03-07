import os
import logging

from config import constants
from config.globals import Globals

logger = None


def initialize_logger():
    global logger

    log_dir = os.path.join(constants.PACKAGE_DIR, "log")
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, "utmd.log")
    logger = logging.getLogger()
    logger.setLevel(constants.LOG_LEVEL)

    while logger.hasHandlers():
        logger.removeHandler(logger.handlers[0])

    formatter = logging.Formatter(
        "[%(asctime)s|%(levelname)-7s][%(name)s %(lineno)d %(funcName)s] %(message)s ",
        datefmt='%Y-%m-%d %H:%M:%S')

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_file, mode="a")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    Globals.set_logger(logger)
    logger.info("Logger initialized.")

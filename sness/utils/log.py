import logging
import time


def config(level):
    """
    Config custom logging
    """

    formatter = logging.Formatter(
        '[%(levelname)s][%(asctime)s][%(filename)-15s][%(lineno)4d] - %(message)s'
    )
    formatter.converter = time.gmtime

    channel = logging.StreamHandler()
    channel.setFormatter(formatter)

    logger = logging.getLogger(__name__)
    logger.setLevel(level)
    logger.addHandler(channel)

    logging.getLogger("py4j").setLevel(logging.ERROR)

    return logger


def get_logger():
    """
    Get the global logger
    """
    global LOGGER
    global LEVEL
    LOGGER.setLevel(LEVEL)
    return LOGGER


LEVEL = 'INFO'
LOGGER = config(LEVEL)

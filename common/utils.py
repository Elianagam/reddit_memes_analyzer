import os
import logging
from configparser import ConfigParser
from common.constants import LOG_LEVEL


LOG_LEVEL_OPTS = ('CRITICAL', 'DEBUG', 'ERROR', 'INFO', 'NOTSET', 'WARNING')
SET_LOG_LEVEL = LOG_LEVEL if LOG_LEVEL in LOG_LEVEL_OPTS else 'INFO'

logger = logging.getLogger('reddit')
_set = False


def initialize_log(log_level=None):
    """
    Python custom logging initialization
    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    # Avoid multiple calls to initialize_log to cause registering multiple logging handlers
    global _set
    if not _set:
        logger.setLevel(level=log_level or SET_LOG_LEVEL)
        fh = logging.StreamHandler()
        fh_formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s')
        fh.setFormatter(fh_formatter)
        logger.addHandler(fh)
        _set = True


def initialize_config(params=[]):
    config = ConfigParser(os.environ)
    config_params = {}
    try:
        for value in params:
            config_params[value] = config["DEFAULT"][value]
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))
    return config_params


def chunked(iterable, n):
    chunk = []
    for i, line in enumerate(iterable):
        if (i % n == 0 and i > 0):
            yield chunk
            chunk = []
        chunk.append(line)

    if chunk:
        yield chunk

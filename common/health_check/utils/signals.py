from functools import wraps
import signal
import logging

logger = logging.getLogger(__name__)


class SigTermException(Exception):
    pass


def sigtermhandler(signum, frame):
    raise SigTermException


def register_handler():
    signal.signal(signal.SIGTERM, sigtermhandler)


def handles_sigterm(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except SigTermException:
            logger.info("Received SIGTERM")
            pass

    return wrapper

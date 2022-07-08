from retry import retry
from pika.exceptions import AMQPConnectionError
from common.utils import logger
import pika
import os

CONNECTION_RETRY_DELAY = int(os.environ.get('CONNECTION_RETRY_DELAY', 5))
CONNECTION_RETRY_BACKOFF = int(os.environ.get('CONNECTION_RETRY_BACKOFF', 2))
CONNECTION_RETRIES = int(os.environ.get('CONNECTION_RERIES', 5))


@retry(exceptions=AMQPConnectionError, delay=CONNECTION_RETRY_DELAY, backoff=CONNECTION_RETRY_BACKOFF, tries=CONNECTION_RETRIES, logger=logger)
def _connect_retry(host, **kwargs):
    """
    Connect to a rabbitmq with a retry mechanism

    We'll run rabbit and its client as docker containers. Since the `depends_on` only tests the container being up and not
    the actual service, we have to implement a retry mechanism so clients keep trying to connnect while the rabbitmq service
    starts.

    Currently we use the retry lib (not builtin) with a default delay of 5 seconds and 5 max retries. Each retry will double
    the time before the next try.
    """
    return pika.BlockingConnection(pika.ConnectionParameters(host=host, **kwargs))


def connect_retry(host='rabbitmq', **kwargs):
    try:
        return _connect_retry(host=host, **kwargs)
    except AMQPConnectionError:
        return None

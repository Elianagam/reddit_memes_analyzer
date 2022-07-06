from utils.connections import connect_retry
from utils import get_container_name
from common.health_check.monitored import MonitoredMixin
from common.health_check.utils.signals import register_handler, SigTermException
from multiprocessing import Queue
import time
import json
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


logging.basicConfig(
    format='[%(asctime)s] %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)


class SampleNode(MonitoredMixin):
    def __init__(self):
        self.queue = Queue()
        register_handler()
        super().__init__()

    def run(self):
        self.start()
        try:
            self.queue.get()
        except SigTermException:
            logger.info("Recibo Sigterm")

        self.terminate()
        self.join()


def main2():
    conn = connect_retry()
    channel = conn.channel()
    node_name = get_container_name()
    logger.info(f"Inicio el nodo {node_name}")
    channel.exchange_declare(exchange='health_check', exchange_type='topic', auto_delete=True)

    while True:
        logger.info("Mando heartbeat")
        msg = json.dumps({'source': node_name})
        channel.basic_publish(exchange='health_check', routing_key='health_check', body=msg)
        time.sleep(2)


if __name__ == '__main__':
    logger.info("Inicio dummy")
    x = SampleNode()
    x.run()

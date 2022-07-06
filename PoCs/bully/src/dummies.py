from utils.connections import connect_retry
from utils import get_container_name
from common.health_check.monitored import MonitoredMixin
from multiprocessing import Queue
import time
import json
import logging

logger = logging.getLogger('carlitos')
logger.setLevel(logging.INFO)


logging.basicConfig(
    format='[%(asctime)s] %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

class SampleNode(MonitoredMixin):
    def __init__(self):
        self.queue = Queue()
        super().__init__()

    def run(self):
        self.start()
        self.queue.get()
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

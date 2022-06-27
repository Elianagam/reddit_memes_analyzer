from common.health_check.utils.connections import connect_retry
from common.health_check.utils import get_container_name

import os
import json
import time
from multiprocessing import Process

HEALTHBEAT_DELAY = int(os.environ.get("HEALTHBEAT_DELAY", 2))


class MonitoredMixin(object):
    def __init__(self):
        self.monitoring_process = None

    def run(self):
        conn = connect_retry()
        channel = conn.channel()
        node_name = get_container_name()
        channel.exchange_declare(exchange='health_check', exchange_type='topic')

        while True:
            msg = json.dumps({'source': node_name})
            channel.basic_publish(exchange='health_check', routing_key='health_check', body=msg)
            time.sleep(HEALTHBEAT_DELAY)

    def start(self):
        process = Process(target=self.run)
        process.daemon = True
        process.start()
        self.monitoring_process = process

    def terminate(self):
        self.monitoring_process.terminate()

    def join(self):
        self.monitoring_process.join()

from common.health_check.utils.connections import connect_retry
from common.health_check.utils import get_container_name
from common.health_check.constants import HEALTHBEAT_DELAY
from common.health_check.utils.signals import register_handler, SigTermException
import os
import json
import time
import logging
from multiprocessing import Process


class MonitoredMixin(object):
    def __init__(self):
        self.monitoring_process = None
        self.node_name = get_container_name()

    def run(self):
        register_handler()
        conn = connect_retry()
        channel = conn.channel()
        channel.exchange_declare(exchange='health_check', exchange_type='topic', auto_delete=True)
        try:
            while True:
                logging.debug("%s Sending Heartbeat", self.node_name)
                msg = json.dumps({'source': self.node_name})
                channel.basic_publish(exchange='health_check', routing_key='health_check', body=msg)
                time.sleep(HEALTHBEAT_DELAY)
        except SigTermException:
            pass
        channel.close()
        conn.close()

    def start(self):
        logging.info("%s Starting Heartbeat", self.node_name)
        process = Process(target=self.run)
        process.daemon = True
        process.start()
        self.monitoring_process = process

    def terminate(self):
        logging.info("Terminating Heartbeat")
        self.monitoring_process.terminate()

    def join(self):
        self.monitoring_process.join()
        logging.info("Terminated Heartbeat")

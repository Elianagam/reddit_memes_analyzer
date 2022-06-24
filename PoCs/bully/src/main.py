import logging
import subprocess
import os
import sys
import time
from utils.connections import connect_retry
from utils import get_node_id
from constants import NODE_QUANTITY

SLEEP_SECONDS = 10


class ClusterNode(object):
    def __init__(self):
        self.node_id = get_node_id()
        self.conn = connect_retry()
        self.channel = self.conn.channel()

    def setup_rabbit(self):
        self.exchange_declare(exchange='election', exchange_type='topic')
        for i in range(1, NODE_QUANTITY+1):
            self.channel.queue_declare(queue=f'node_{i}', exchange='election', routing_key=str(i))

    def run(self):
        while True:
            print(f"Hello from {self.node_id}")
            time.sleep(SLEEP_SECONDS)


if __name__ == '__main__':
    node = ClusterNode()
    node.run()

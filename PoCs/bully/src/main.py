"""
Prueba de Concepto para una clase que implemente el algoritmo de lider

# TODO:
 [ ] - El nodo con mayor ID se autoproclame lider al inciarse
 [ ] - Los nodos deben leer actualizaciones del lider y timeoutear en caso de no recibir cada N tiempo
 [ ] - Ver como hacer la elección
"""
import logging
import subprocess
import os
import sys
import logging
import time
from utils.connections import connect_retry
from utils import get_node_id
from constants import NODE_QUANTITY
from multiprocessing import Process

SLEEP_SECONDS = 3
HEARTBEAT_DELAY = 5
HEARTBEAT_LIMIT = HEARTBEAT_DELAY * 2

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
)


class ClusterNode(object):
    def __init__(self):
        self.node_id = get_node_id()
        self.conn = connect_retry()
        self.channel = self.conn.channel()
        self.queue = None
        self.leader = True
        self.heartbeat_process = None
        self.last_hb_processed = None
        self.leader = None

    def setup_rabbit(self):
        self.channel.exchange_declare(exchange='election', exchange_type='topic')
        self.queue = f'node_{self.node_id}'
        for i in range(1, NODE_QUANTITY+1):
            queue_name = f'node_{i}'
            print(f"Creando {queue_name}")
            self.channel.queue_declare(queue=queue_name)
            self.channel.queue_bind(queue=queue_name, exchange='election', routing_key=str(i))

    def check_init(self):
        if self.node_id == NODE_QUANTITY:
            self.broadcast_victory()
            self.init_heartbeat()

    def broadcast_victory(self):
        for i in range(1, NODE_QUANTITY+1):
            if i != self.node_id:
                self.channel.basic_publish(exchange='election', routing_key=str(i), body=f'victory:{self.node_id}')

    def get_message(self):
        _, _, body = self.channel.basic_get(queue=self.queue)
        return body.decode() if body else body

    def heartbeat(self):
        while True:
            for i in range(1, NODE_QUANTITY+1):
                if i != self.node_id:
                    self.channel.basic_publish(exchange='election', routing_key=str(i), body=f'heartbeat:{self.node_id}:{time.time()}')
            time.sleep(HEARTBEAT_DELAY)

    def init_heartbeat(self):
        print("Iniciando heartbeat")
        self.heartbeat_process = Process(target=self.heartbeat)
        self.heartbeat_process.daemon = True
        self.heartbeat_process.start()

    def process_msg(self, msg):
        if msg[:9] == 'heartbeat':
            hb, node_id, ts = msg.split(':')
            logging.info("Recibi heartbeat %r", ts)
            now = time.time()
            self.last_hb_processed = max(now, float(ts))

    def check_heartbeat(self):
        now = time.time()
        delta = (now - self.last_hb_processed) if self.last_hb_processed else None
        logging.info(f"El delta es {delta}")
        if delta and delta > HEARTBEAT_LIMIT:
            print("El lider se murio xD")

    def run(self):
        while True:
            msg = self.get_message()
            if not msg:
                print(f"{self.node_id} No recibio nada")
                time.sleep(SLEEP_SECONDS)
                self.check_heartbeat()
            else:
                print(f"{self.node_id} recibio {msg}")
                self.process_msg(msg)
                self.check_heartbeat()


if __name__ == '__main__':
    node = ClusterNode()
    node.setup_rabbit()
    node.check_init()
    node.run()

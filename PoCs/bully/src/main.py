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
import inspect
import json
from utils.connections import connect_retry
from utils import get_node_id
from constants import NODE_QUANTITY, LOG_LEVEL
from multiprocessing import Process
from enum import Enum

SLEEP_SECONDS = 1
HEARTBEAT_DELAY = 5
HEARTBEAT_LIMIT = HEARTBEAT_DELAY * 2

logger = logging.getLogger('carlitos')
logger.setLevel(logging.getLevelName(LOG_LEVEL))


logging.basicConfig(
    format='[%(asctime)s] %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)


class NodeState(Enum):
    UNKNOWN = 'unknown'
    OK = 'ok'
    ELECTION_INIT = 'election_init'
    ELECTION_WAITING_OK = 'election_oks'
    ELECTION_WAITING_VICTORY = 'election_victory'


STATES_MAPPING = {e.value: e for e in NodeState}


class ClusterNode(object):
    def __init__(self):
        self.node_id = get_node_id()
        self.conn = None
        self.channel = None
        self.queue = None
        self.leader = False
        self.heartbeat_process = None
        self.last_hb_processed = None
        self.checkpoint = None
        self.leader_id = None
        self.state = NodeState.UNKNOWN
        self.state_mapping = self.populate_states()
        self.dataset = {}
        logger.debug('%s iniciado', self.node_id)

    def populate_states(self):
        states = {}
        for name, func in inspect.getmembers(self.__class__, inspect.isfunction):
            if name.startswith('s_'):
                _name = name[2:]
                if _name in STATES_MAPPING:
                    states[STATES_MAPPING[_name]] = getattr(self, name)
        return states

    def connect(self):
        self.conn = connect_retry()
        self.channel = self.conn.channel()

    def setup_rabbit(self):
        self.channel.exchange_declare(exchange='health_check', exchange_type='topic')
        self.channel.queue_declare(queue='health_check')
        self.channel.queue_bind(queue='health_check', exchange='health_check', routing_key='health_check')
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

    @property
    def all_ids(self):
        for i in range(1, NODE_QUANTITY+1):
            if i != self.node_id:
                yield i

    @property
    def major_ids(self):
        for i in range(1, NODE_QUANTITY+1):
            if i > self.node_id:
                yield i

    def send_to(self, ids, message):
        for i in ids:
            self.channel.basic_publish(exchange='election', routing_key=str(i), body=json.dumps(message))

    def broadcast_victory(self):
        msg = {
            'type': 'victory',
            'node_id': self.node_id
        }
        self.send_to(self.all_ids, msg)

    # Cambiar por consume que permite timeout
    def get_message(self):
        _, _, body = self.channel.basic_get(queue=self.queue)
        return json.loads(body) if body else body

    def health_check(self, conn):
        # Channel.consume es bloqueante
        channel = conn.channel()
        logger.info("%s escuchando mensajes del heath check")
        for _, _, msg in channel.consume(queue='health_check'):
            logger.info("Recibi %r", msg)

    def init_healthcheck(self):
        print("Iniciando heartbeat")
        conn = connect_retry()
        self.heartbeat_process = Process(target=self.health_check, args=(conn,))
        self.heartbeat_process.daemon = True
        self.heartbeat_process.start()

    def heartbeat(self):
        ids = list(self.all_ids)
        while True:
            msg = f'heartbeat:{self.node_id}:{time.time()}'
            self.send_to(ids, msg)
            logger.debug("Sending heartbeat")
            time.sleep(HEARTBEAT_DELAY)

    def init_heartbeat(self):
        print("Iniciando heartbeat")
        self.heartbeat_process = Process(target=self.heartbeat)
        self.heartbeat_process.daemon = True
        self.heartbeat_process.start()

    def process_msg(self, msg):
        if msg[:9] == 'heartbeat':
            hb, node_id, ts = msg.split(':')
            logger.debug("Recibi heartbeat %r", ts)
            now = time.time()
            self.last_hb_processed = max(now, float(ts))

    def check_heartbeat(self):
        now = time.time()
        delta = (now - self.last_hb_processed) if self.last_hb_processed else None
        logging.debug(f"El delta es {delta}")
        if delta and delta > HEARTBEAT_LIMIT:
            print("El lider se murio xD")

    def proclamate_leader(self):
        logger.info("Broadcasteo")
        self.broadcast_victory()
        logger.info("cambio vars")
        self.leader = True
        self.state = NodeState.OK
        #TODO: Descomentar
        self.init_heartbeat()
        logger.info("Por hacer healthcheck")
        self.init_healthcheck()

    def start_election(self):
        msg = {
            'type': 'election_init',
            'source': self.node_id,
        }
        majors = list(self.major_ids)
        for major_id in majors:
            self.dataset[int(major_id)] = {'status': 'sent', 'ts': time.time()}
        self.send_to(majors, msg)
        logging.info("De 'start_election' paso a 'waiting_ok'")
        self.state = NodeState.ELECTION_WAITING_OK

    def set_winner(self, node_id):
        logger.info("Setting %d as winner", node_id)
        # Blanqueo mi data
        self.dataset = {}
        self.checkpoint = time.time()
        logger.debug("Paso a OK, ya se setó un lider")
        self.state = NodeState.OK
        self.leader_id = int(node_id)

    ### Definir las funciones que van a interceder acorde el estado
    def s_unknown(self, msg):
        """
        Método que va a actuar cuando un nodo recién se levanta y no sabe el estado del cluster
        """
        pass

    def s_election_oks(self, msg):
        """
        El nodo inicio una elección y está esperando los OKs del init de eleccion
        """
        if msg:
            msgtype = msg['type']
            if msgtype == 'election_ok':
                source = int(msg['source'])
                self.dataset.pop(source, None)
                if not self.dataset:
                    # Ya recibí el OK de todos mis superiores, me quedo esperando el victory
                    self.checkpoint = time.time()
                    logging.debug("De election_oks paso a waiting_victory")
                    self.state = NodeState.ELECTION_WAITING_VICTORY
            if msgtype == 'victory':
                winner =msg['node_id']
                self.set_winner(winner)

    def s_election_victory(self, msg):
        """
        Espero la respuesta de un victory
        """
        pass

    def s_ok(self, msg):
        """
        Cuando está todo ok
        """
        logger.debug("Espero un heartbeat")
    ###

    def first_cycle(self):
        # Cuando inicio no conozco el estado del cluster, así que por las dudas inicio una elección

        # Veo todos los IDs superiores
        major_ids = list(self.major_ids)
        logger.info("INICIANDO")
        if not major_ids:
            # Soy el mayor nodo, me autoproclamo lider
            logger.debug("%s se autoproclama liderr", self.node_id)
            print("llamando al proclamate leader")
            self.proclamate_leader()
        else:
            # Hay mayores, por lo que hay que hacer una election
            logger.debug("%s inicio una aleccion")
            self.start_election()

    def run(self):
        self.first_cycle()
        timeout = SLEEP_SECONDS
        for _, _, msg in self.channel.consume(queue=self.queue, inactivity_timeout=timeout):
            msg = json.loads(msg) if msg else None

            f = self.state_mapping[self.state]
            logger.debug("LLamo a %r con %r", f, msg)
            f(msg)


if __name__ == '__main__':
    node = ClusterNode()
    node.connect()
    node.setup_rabbit()
    #node.check_init()
    node.run()

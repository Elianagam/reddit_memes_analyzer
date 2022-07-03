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
from common.health_check.utils.connections import connect_retry
from common.health_check.utils import get_node_id, start_container
from common.health_check.constants import (
    ELECTION_TIMEOUT,
    HEALTHCHECK_NODE_TIMEOUT,
    HEALTHCHECK_READ_TIMEOUT,
    HEARTBEAT_SLEEP,
    HEARTBEAT_TIMEOUT,
    LOG_LEVEL,
    NODE_QUANTITY,
    PERSISTENCE_COMPLETE_PATH,
    SLEEP_SECONDS,
)
from multiprocessing import Process
from enum import Enum

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
        self.healthcheck_process = None
        self.last_hb_processed = None
        self.checkpoint = None
        self.leader_id = None
        self.state = NodeState.UNKNOWN
        self.state_mapping = self.populate_states()
        self.dataset = {}
        self.health_monitoring = {}
        logger.debug('%s iniciado', self.node_id)

    def populate_states(self):
        states = {}
        for name, func in inspect.getmembers(self.__class__, inspect.isfunction):
            if name.startswith('s_'):
                _name = name[2:]
                if _name in STATES_MAPPING:
                    states[STATES_MAPPING[_name]] = getattr(self, name)
        return states

    def save(self):
        with open(PERSISTENCE_COMPLETE_PATH, 'w+') as f:
            json.dump(self.health_monitoring, f)

    def load(self):
        with open(PERSISTENCE_COMPLETE_PATH, 'r') as f:
            try:
                self.health_monitoring = json.load(f)
            except:
                self.health_monitoring = {}

    def connect(self):
        self.conn = connect_retry()
        self.channel = self.conn.channel()

    def setup_rabbit(self):
        self.channel.exchange_declare(exchange='health_check', exchange_type='topic')
        self.channel.queue_declare(queue='health_check')
        self.channel.queue_bind(queue='health_check', exchange='health_check', routing_key='health_check')
        self.channel.exchange_declare(exchange='election', exchange_type='topic')
        self.queue = f'node_{self.node_id}'

        self.channel.queue_declare(queue=self.queue, exclusive=True)
        self.channel.queue_bind(queue=self.queue, exchange='election', routing_key=str(self.node_id))
        #for i in range(1, NODE_QUANTITY+1):
        #    queue_name = f'node_{i}'
        #    print(f"Creando {queue_name}")
        #    self.channel.queue_declare(queue=queue_name)
        #    self.channel.queue_bind(queue=queue_name, exchange='election', routing_key=str(i))

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

    def send_to(self, ids, message, channel=None):
        channel = channel if channel else self.channel
        for i in ids:
            self.channel.basic_publish(exchange='election', routing_key=str(i), body=json.dumps(message))

    def broadcast_victory(self, ids=None):
        targets = ids or self.all_ids

        msg = {
            'type': 'victory',
            'node_id': self.node_id
        }
        self.send_to(targets, msg)

    # Cambiar por consume que permite timeout
    def get_message(self):
        _, _, body = self.channel.basic_get(queue=self.queue)
        return json.loads(body) if body else body

    # TODA ESTA PARTE ES DEL HEATH-CHECK PER SE
    def restart_node(self, node):
        logger.warning("Restarteando %r", node)
        start_container(name=node)

    def check_node_healths(self):
        now = time.time()
        logger.info("2) Dataset es %r", self.health_monitoring)
        for node, ts in self.health_monitoring.items():
            if now - ts > HEALTHCHECK_NODE_TIMEOUT:
                logger.info("Ahora estoy en %r, el ts es %r", now, ts)
                self.restart_node(node)
                # Le vuelvo a setear el checkpoint para que no reintente muy rapido
                self.health_monitoring[node] = now
                self.save()

    def health_check(self, conn):
        # Channel.consume es bloqueante
        channel = conn.channel()
        logger.info("%s escuchando mensajes del heath check")
        for _, _, msg in channel.consume(queue='health_check', inactivity_timeout=HEALTHCHECK_READ_TIMEOUT):
            if msg:
                logger.info("Recibi %r", msg)
                msg = json.loads(msg)
                source = msg['source']
                now = time.time()
                self.health_monitoring[source] = now
                self.save()
                logger.info("Actualizo %r con %r", source, now)
                logger.info("Checkeo...valor actual %r", self.health_monitoring[source])
            logger.info("1) Dataset es %r", self.health_monitoring)
            self.check_node_healths()

    def init_healthcheck(self):
        if self.healthcheck_process:
            return
        print("Iniciando heartbeat")
        conn = connect_retry()
        self.healthcheck_process = Process(target=self.health_check, args=(conn,))
        self.healthcheck_process.daemon = True
        self.healthcheck_process.start()

    # FIN DEL HEALTH CHECK

    def heartbeat(self, conn):
        channel = conn.channel()
        ids = list(self.all_ids)
        while True:
            msg = {
                'type': 'heartbeat'
            }
            #msg = f'heartbeat:{self.node_id}:{time.time()}'
            self.send_to(ids, msg, channel=channel)
            logger.debug("Sending heartbeat")
            time.sleep(HEARTBEAT_SLEEP)

    def init_heartbeat(self):
        if self.heartbeat_process:
            return
        print("Iniciando heartbeat")
        conn = connect_retry()
        self.heartbeat_process = Process(target=self.heartbeat, args=(conn,))
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
        logger.debug(f"El delta es {delta}")
        if delta and delta > HEARTBEAT_TIMEOUT:
            print("El lider se murio xD")

    def proclamate_leader(self):
        logger.info("Broadcasteo")
        self.broadcast_victory()
        logger.info("cambio vars")
        self.leader = True
        self.state = NodeState.OK
        #TODO: Descomentar
        self.init_heartbeat()
        self.load()
        logger.info("Cargando la info persistida, la misma es %r", self.health_monitoring)
        logger.info("Por hacer healthcheck")
        self.init_healthcheck()

    def demote(self):
        """
        Era el lider y ahora ya no, tengo que terminar con los subprocessos
        """
        self.heartbeat_process.terminate()
        self.heartbeat_process.join()

        self.healthcheck_process.terminate()
        self.healthcheck_process.join()

        self.heartbeat_process = None
        self.healthcheck_process = None

    def set_checkpoint(self):
        self.checkpoint = time.time()

    def get_delta(self):
        return time.time() - self.checkpoint if self.checkpoint else -1

    def exceeded_election_limit(self):
        """
        La función verifica si se sobrepasó el tiempo dado para la elección o no
        """
        distance_to_top = NODE_QUANTITY - self.node_id
        allowed_timeout = ELECTION_TIMEOUT * distance_to_top
        return self.get_delta() > allowed_timeout

    def exceeded_heartbeat_limit(self):
        return self.get_delta() > HEARTBEAT_TIMEOUT

    def start_election(self):
        msg = {
            'type': 'election_init',
            'source': self.node_id,
        }
        majors = list(self.major_ids)
        for major_id in majors:
            self.dataset[int(major_id)] = {'status': 'sent', 'ts': time.time()}
        self.send_to(majors, msg)
        logger.info("De 'start_election' paso a 'waiting_ok'")
        self.state = NodeState.ELECTION_WAITING_OK
        self.set_checkpoint()

    def set_winner(self, node_id):
        logger.info("Setting %d as winner", node_id)
        if self.leader:
            logger.warning("HAGO UN DEMOTE >>>>")
            self.demote()
        # Blanqueo mi data
        self.dataset = {}
        self.leader = False
        logger.debug("Paso a OK, ya se setó un lider")
        self.state = NodeState.OK
        self.set_checkpoint()
        self.leader_id = int(node_id)

    def process_victory(self, msg):
        """
        Process a Victory message
        """
        winner = msg['node_id']
        self.set_winner(winner)

    def process_election(self, msg):
        source = msg['source']
        # Algun nodo inició una elección, le respondo que OK e inicio mi propia elección
        msg = {
            'type': 'election_ok',
            'source': self.node_id,
        }
        logger.info("Recibí election de %d y respondo ok", source)
        self.send_to([source, ], msg)
        self.start_election()

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
                logger.debug("Recibí un ok de %d", source)
                self.dataset.pop(source, None)
                if not self.dataset:
                    # Ya recibí el OK de todos mis superiores, me quedo esperando el victory
                    self.set_checkpoint()
                    logger.debug("De election_oks paso a waiting_victory")
                    self.state = NodeState.ELECTION_WAITING_VICTORY
                if self.exceeded_election_limit():
                    self.proclamate_leader()
            if msgtype == 'victory':
                logger.info("Espero mensaje de OKs y recibi uno de victoria %r", msg)
                self.process_victory(msg)
        else:
            # No recibi ningun mensaje, timeouteó el consume, debo chequear si se expiró el tiempo
            # de espera para los mensajes de los nodos 'superiores'
            if self.exceeded_election_limit():
                self.proclamate_leader()

    def s_election_victory(self, msg):
        """
        Espero la respuesta de un victory. Llego acá porque inicié una elección y ya recibí todos
        los OKs
        """
        if msg:
            msgtype = msg['type']
            if msgtype == 'victory':
                logger.info("Espero mensaje de victoria y recibi uno %r", msg)
                self.process_victory(msg)
        else:
            # No recibi ningun mensaje, timeouteó el consume, debo chequear si se expiró el tiempo
            # de espera para los mensajes de los nodos 'superiores'
            if self.exceeded_election_limit():
                self.proclamate_leader()

    def s_ok(self, msg):
        """
        Cuando está todo
         - Espera Heartbeat
         - Procesa elecciones de otros nodos
        """
        if msg:
            msgtype = msg['type']
            if msgtype == 'election_init':
                if self.leader:
                    source = msg['source']
                    # Soy el lider no inicio ninguna elección, le refuerzo mi victoria
                    self.broadcast_victory(ids=[source])
                else:
                    self.process_election(msg)
            elif msgtype == 'heartbeat':
                self.set_checkpoint()
            elif msgtype == 'victory':
                self.process_victory(msg)
        elif not self.leader:
            # Me fijo cuando fue el ultimo heartbeat que recibi
            if self.exceeded_heartbeat_limit():
                logger.warning("Se perdieron los heartbeats, inicio una elección")
                self.start_election()

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

"""
Módulo que maneja los nodos de HealthChecking
"""
import logging
import subprocess
import os
import sys
import signal
import logging
import time
import inspect
import json
from common.health_check.utils.connections import connect_retry
from common.health_check.utils.signals import register_handler, SigTermException
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
    VICTORY_TIMEOUT,
)
from multiprocessing import Process
from enum import Enum

logger = logging.getLogger(__name__)
logger.setLevel(logging.getLevelName(LOG_LEVEL))


logging.basicConfig(
    format='[%(asctime)s] %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)


class NodeState(Enum):
    """
    Representa todos los estados posibles que puede tener un nodo
    """
    UNKNOWN = 'unknown'
    OK = 'ok'
    #ELECTION_INIT = 'election_init'
    ELECTION_WAITING_OK = 'election_oks'
    ELECTION_WAITING_VICTORY = 'election_victory'
    VICTORY_WAIT = 'victory_wait'


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
        logger.info('%s iniciado', self.node_id)
        register_handler()
        #signal.signal(signal.SIGTERM, self.exit_gracefully)

    def populate_states(self):
        """
        Mappea cada uno de los estados del nodo a un método que procesa mensajes

        Por ejemplo si el nodo se encuentra en NodeState.OK (estado estable), el método que
        procesará los mensajes será `s_ok`
        """
        states = {}
        for name, func in inspect.getmembers(self.__class__, inspect.isfunction):
            if name.startswith('s_'):
                _name = name[2:]
                if _name in STATES_MAPPING:
                    states[STATES_MAPPING[_name]] = getattr(self, name)
        return states

    def save(self):
        """
        Persiste la información de monitoreo en disco
        """
        with open(PERSISTENCE_COMPLETE_PATH, 'w+') as f:
            json.dump(self.health_monitoring, f)

    def load(self):
        """
        Carga la información de monitoreo del disco
        """
        try:
            with open(PERSISTENCE_COMPLETE_PATH, 'r') as f:
                self.health_monitoring = json.load(f)
        except:
            self.health_monitoring = {}

    def delete_persistence(self):
        if os.path.exists(PERSISTENCE_COMPLETE_PATH):
            try:
                os.remove(PERSISTENCE_COMPLETE_PATH)
            except:
                pass

    def connect(self):
        self.conn = connect_retry()
        self.channel = self.conn.channel()

    def setup_rabbit(self):
        self.channel.exchange_declare(exchange='health_check', exchange_type='topic', auto_delete=True)
        self.channel.queue_declare(queue='health_check')
        self.channel.queue_bind(queue='health_check', exchange='health_check', routing_key='health_check')
        self.channel.exchange_declare(exchange='election', exchange_type='topic', auto_delete=True)
        self.queue = f'node_{self.node_id}'

        self.channel.queue_declare(queue=self.queue, exclusive=True)
        self.channel.queue_bind(queue=self.queue, exchange='election', routing_key=str(self.node_id))

    def teardown(self):
        self.channel.queue_delete(self.queue)
        try:
            self.channel.queue_delete(queue='health_check')
        except ValueError:
            logger.warning("ValueError found when deleting health_check queue")
        self.channel.close()
        self.conn.close()
        self.delete_persistence()

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

    @staticmethod
    def ack_message(tag, channel):
        channel.basic_ack(delivery_tag=tag)

    def send_to(self, ids, message, channel=None):
        channel = channel if channel else self.channel
        for i in ids:
            channel.basic_publish(exchange='election', routing_key=str(i), body=json.dumps(message))

    def broadcast_victory(self, ids=None):
        targets = ids or self.all_ids

        msg = {
            'type': 'victory',
            'node_id': self.node_id
        }
        self.send_to(targets, msg)

    @staticmethod
    def restart_node(node):
        logger.info("Restarteando nodo caido %r", node)
        start_container(name=node)

    def check_node_healths(self):
        """
        Itera sobre el dataset de notods y verifica si alguno superó el timeout, en caso de timeout
        se llama al método que reinicia el nodo
        """
        now = time.time()
        logger.debug("Current health monitoring %r", self.health_monitoring)
        for node, ts in self.health_monitoring.items():
            if now - ts > HEALTHCHECK_NODE_TIMEOUT:
                self.restart_node(node)
                # Le vuelvo a setear el checkpoint para que no reintente muy rapido
                self.health_monitoring[node] = now
                self.save()

    def health_check(self):
        """
        Realiza el Health Check de los nodos a monitorear
        """
        conn = connect_retry()
        channel = conn.channel()
        logger.info("%s escuchando mensajes del heath check")
        try:
            for method, _, msg in channel.consume(queue='health_check',
                                                  auto_ack=False,
                                                  inactivity_timeout=HEALTHCHECK_READ_TIMEOUT):
                if msg:
                    msg = json.loads(msg)
                    logger.debug("HealthCheck recibe %r", msg)
                    source = msg['source']
                    now = time.time()
                    self.health_monitoring[source] = now
                    # Guardo el set actualizado de datos
                    self.save()
                # Al final de cada ciclo valido el estado de los nodos monitoreados
                self.check_node_healths()
                self.ack_message(tag=method.delivery_tag, channel=channel)
        except SigTermException:
            pass

        # Teardown
        channel.close()
        conn.close()

    def init_healthcheck(self):
        """
        Inicia el subproceso encargargado de escuchar los mensajes de Health Check de los nodos a
        monitorear
        """
        if self.healthcheck_process:
            return
        logger.info("Iniciando healthcheck")
        self.healthcheck_process = Process(target=self.health_check)
        self.healthcheck_process.daemon = True
        self.healthcheck_process.start()

    def heartbeat(self):
        """
        Se envia un heartbeat a todos los nodos del cluster
        """
        conn = connect_retry()
        channel = conn.channel()
        ids = list(self.all_ids)
        try:
            while True:
                msg = {
                    'type': 'heartbeat'
                }
                self.send_to(ids, msg, channel=channel)
                logger.debug("Sending heartbeat")
                time.sleep(HEARTBEAT_SLEEP)
        except SigTermException:
            pass

        # Teardown
        channel.close()
        conn.close()

    def init_heartbeat(self):
        """
        Se inicia un subproceso encargado de enviar un heartbeat a todos los nodos del cluster
        """
        if self.heartbeat_process:
            return
        logger.info("Iniciando heartbeat")
        self.heartbeat_process = Process(target=self.heartbeat)
        self.heartbeat_process.daemon = True
        self.heartbeat_process.start()

    def start_leader_duties(self):
        self.init_heartbeat()
        self.load()
        logger.debug("Cargando la info persistida, la misma es %r", self.health_monitoring)
        self.init_healthcheck()
        self.state = NodeState.OK

    def proclamate_leader(self):
        """
        El nodo se autoproclama lider
        """
        self.broadcast_victory()
        self.leader = True
        self.state = NodeState.VICTORY_WAIT
        self.set_checkpoint()

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

    def exceeded_victory_wait(self):
        return self.get_delta() > VICTORY_TIMEOUT

    def exceeded_heartbeat_limit(self):
        return self.get_delta() > HEARTBEAT_TIMEOUT

    def start_election(self):
        """
        Inicia un proceso de eleccion
        """
        msg = {
            'type': 'election_init',
            'source': self.node_id,
        }
        majors = list(self.major_ids)
        for major_id in majors:
            self.dataset[int(major_id)] = {'status': 'sent', 'ts': time.time()}
        self.send_to(majors, msg)
        logger.debug("De 'start_election' paso a 'waiting_ok'")
        self.state = NodeState.ELECTION_WAITING_OK
        self.set_checkpoint()

    def set_winner(self, node_id):
        """
        Seteo el ganador de la elección
        """
        logger.info("Setting %d as winner", node_id)
        if self.leader:
            # Era lider y ya no
            self.demote()
            self.send_to([node_id], {'type': 'victory_ok', 'source': self.node_id})
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
        logger.debug("Recibí election de %d y respondo ok", source)
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
                logger.debug("Espero mensaje de victoria y recibi uno %r", msg)
                self.process_victory(msg)
        else:
            # No recibi ningun mensaje, timeouteó el consume, debo chequear si se expiró el tiempo
            # de espera para los mensajes de los nodos 'superiores'
            if self.exceeded_election_limit():
                self.proclamate_leader()

    def s_victory_wait(self, msg):
        """
        Soy el nuevo lider y espero a recibir un OK del lider actual para evitar que haya
        dos lideres en simultaneo.
        """
        if msg:
            msgtype = msg['type']
            if msgtype == 'victory_ok':
                source = msg['source']
                logger.debug("Recibo mensaje del lider actual: %d", source)
                self.start_leader_duties()
        else:
            if self.exceeded_victory_wait():
                logger.info("Hubo timeout")
                self.start_leader_duties()

    def s_ok(self, msg):
        """
        Cuando está todo ok
         - Espera Heartbeat
         - Procesa elecciones de otros nodos
        """
        if msg:
            msgtype = msg['type']
            if msgtype == 'election_init':
                # Otro nodo inició una elección
                if self.leader:
                    source = msg['source']
                    # Soy el lider no inicio ninguna elección, le refuerzo mi victoria
                    self.broadcast_victory(ids=[source])
                else:
                    # No soy lider, por lo cual proceso la elección
                    self.process_election(msg)
            elif msgtype == 'heartbeat':
                # Recibí un heartbeat, actualizo el checkpoint
                self.set_checkpoint()
            elif msgtype == 'victory':
                # Recibí una victoria
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
            self.proclamate_leader()
        else:
            # Hay mayores, por lo que hay que hacer una election
            logger.debug("%s inicio una aleccion")
            self.start_election()

    def run(self):
        """
        Loop principal del HealthCheck

        Lee de la queue especifica del nodo y procesa mensajes
        """
        # Realiza operaciones específicas para el primer ciclo del nodo
        self.first_cycle()
        timeout = SLEEP_SECONDS

        # Consume de la queue del nodo, en caso de timeout sale con resultado None, None, None
        for method, _, msg in self.channel.consume(queue=self.queue,
                                              auto_ack=False,
                                              inactivity_timeout=timeout):
            msg = json.loads(msg) if msg else None

            # Se le pasa el mensaje a la función adecuada acorde al estado actual del nodo
            f = self.state_mapping[self.state]
            logger.debug("LLamo a %r con %r", f, msg)
            f(msg)
            self.ack_message(tag=method.delivery_tag, channel=self.channel)

def main():
    node = ClusterNode()
    node.connect()
    node.setup_rabbit()
    try:
        node.run()
    except SigTermException:
        pass
    node.teardown()


if __name__ == '__main__':
    main()

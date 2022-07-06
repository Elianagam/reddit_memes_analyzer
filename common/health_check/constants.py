import os
import logging

NODE_QUANTITY = int(os.environ.get('REPLICAS', 1))
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()
PERSISTENCE_PATH = os.environ.get("PERSISTENCE_PATH", '/tmp/persistence/')
PERSISTENCE_FILE = os.environ.get("PERSISTENCE_FILE", 'healthcheck.json')
PERSISTENCE_COMPLETE_PATH = os.path.join(PERSISTENCE_PATH, PERSISTENCE_FILE)
# ELECTION_TIMEOUT: EL tiempo que espera un nodo en un estado de elección hasta que se
# autoproclama como ganador
ELECTION_TIMEOUT = int(os.environ.get('ELECTION_TIMEOUT', 4))
# HERTBEAT_SLEEP: EL tiempo entre heartbeats sucesivos del lider
HEARTBEAT_SLEEP = int(os.environ.get('HEATBEAT_SLEEP', 1))
# HEARTBEAT_TIMEOUT: El tiempo que debe pasar desde el ultimo heartbeat para considerarlo muerto al lider
HEARTBEAT_TIMEOUT = int(os.environ.get("HEARTBEAT_DELAY", 4))
# SLEEP_SECONDS = El tiempo que espera un nodo a que haya un mensaje
SLEEP_SECONDS = int(os.environ.get("SLEEP_DELAY", 1))
# HEALTHCHECK_READ_TIMEOUT: EL tiempo que el lider espera por un mensaje de healthcheck
HEALTHCHECK_READ_TIMEOUT = int(os.environ.get("HEALTHCHECK_READ_TIMEOUT", 2))
# HEALTHCHECK_NODE_TIMEOUT: El tiempo desde el ultimo healthbeat para que el lider considere un nodo muerto
HEALTHCHECK_NODE_TIMEOUT = int(os.environ.get('HEALTHCHECK_NODE_TIMEOUT', 6))
# HEALTHBEAT_DELAY: Tiempo entre 'healthbeats' de un nodo
HEALTHBEAT_DELAY = int(os.environ.get('HEALTHBEAT_DELAY', 2))
# VICTORY_TIMEOUT: Tiempo que espera un lider a recibir el 'ok' del lider actual (de haberlo)
VICTORY_TIMEOUT = int(os.environ.get('VICTORY_TIMEOUT', 2))

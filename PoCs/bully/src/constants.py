import os
import logging

NODE_QUANTITY = int(os.environ.get('REPLICAS', 1))
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()
PERSISTENCE_PATH = os.environ.get("PERSISTENCE_PATH", '/tmp/persistence/')
PERSISTENCE_FILE = os.environ.get("PERSISTENCE_FILE", 'healthcheck.json')
PERSISTENCE_COMPLETE_PATH = os.path.join(PERSISTENCE_PATH, PERSISTENCE_FILE)
